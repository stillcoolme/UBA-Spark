package com.stillcoolme.spark.service.activeuser;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stillcoolme.spark.domain.Task;
import com.stillcoolme.spark.entity.ReqEntity;
import com.stillcoolme.spark.entity.RespEntity;
import com.stillcoolme.spark.service.BaseService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author stillcoolme
 * @date 2019/1/18 22:12
 */
public class UserActiveDegreeAnalyze extends BaseService {

    @Override
    public RespEntity run(ReqEntity req) {

        RespEntity resEntity = new RespEntity();
        // "[{\"taskId\":4}]"
        JSONArray jsonArray = JSONArray.parseArray(req.reqData);
        JSONObject condition = jsonArray.getJSONObject(0);
        // 客户端 submit 传过来 任务taskId，通过taskId去mysql查询出来指定的任务，并获取任务的查询参数
        Long taskid = condition.getLong("taskId");
        if (taskid == null) {
            return null;
        }
        Task task = taskDao.findById(taskid);
        // {"startDate":"2018-10-14 10:00:00","endDate":"2019-11-21 10:00:00"}
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = taskParam.getString("startDate");
        String endDate = taskParam.getString("endDate");

        // 获取两份数据集
        Dataset<Row> userBaseInfo = sparkSession.read().format("json").load("uba-spark/src/main/resources/data/user_base_info.json");
        Dataset<Row> userActionLog = sparkSession.read().format("json").load("uba-spark/src/main/resources/data/user_action_log.json");
/*        userBaseInfo.printSchema();
        userActionLog.printSchema();*/


        // 第一个功能：统计指定时间范围内的访问次数最多的10个用户
        Dataset<Row> userActionLogDS = userActionLog.filter("actionTime >= '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 0");
        // java要先转成RDD！
        JavaRDD<Row> userActionLogRDD = userActionLogDS.toJavaRDD();
        JavaRDD<Row> userBaseInfoRDD = userBaseInfo.toJavaRDD();
        JavaPairRDD<Long, Row> userActionLogPairRDD = userActionLogRDD.mapToPair(row -> {
            return new Tuple2(row.getAs(5), row);
        });
        JavaPairRDD<Long, Row> userBaseInfoPairRDD = userBaseInfoRDD.mapToPair(row -> {
            return new Tuple2<>(row.getAs(0), row);
        });

        // join之后得到的是 <Long, Tuple2<Row, Row>>
        // (80,([80,user80],[null,2016-10-15 15:42:46,0,805,0.0,80]))
        JavaPairRDD joinRDD = userBaseInfoPairRDD.join(userActionLogPairRDD);
        JavaPairRDD<Long, Iterable<Tuple2<Row, Row>>> id2InfoRDD = joinRDD.groupByKey();
        JavaPairRDD<Long, String> id2UsernameRDD = id2InfoRDD.mapToPair(tuple2 -> {
           Iterator<Tuple2<Row, Row>> iterator = tuple2._2.iterator();
           Long count = 0L;
           String username = "";
           while (iterator.hasNext()) {
               Tuple2<Row, Row> tuple = iterator.next();
                if(count < 1){
                    username = tuple._1.getString(1);
                }
                count ++;
           }
           return new Tuple2<Long, String>(count, username);
        });
        JavaPairRDD<Long, String> id2UsernameSortRDD = id2UsernameRDD.sortByKey(false);
        List<Tuple2<Long, String>> result = id2UsernameSortRDD.take(10);
        Map<Long, String> resultMap = new HashMap<>();
        for (Tuple2 tuple: result) {
            Long id = (Long) tuple._1;
            String username = (String) tuple._2;
            resultMap.put(id, username);
            System.out.println(username + "访问次数: " + id);
        }
        /*
         * 看到别人的scala版实现，我居然不会写java版的了。是RDD还是sparksql还是DataSet，极度混乱。。。
         * 1. RDD编程：将DataSet转为JavaRDD，然后用算子mapToPair成tuple2才能join？？？太麻烦了吧受不了。
         * 2. SparkSql：将DataSet注册为表，然后使用sql语句来join。
         *
         userActionLog
                // 第一步：过滤数据，找到指定时间范围内的数据
                .filter("actionTime >= '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 0")
                // 第二步：关联对应的用户基本信息数据
                .join(userBaseInfo, userActionLog("userId") === userBaseInfo("userId"))
                // 第三部：进行分组，按照userid和username
                .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
                // 第四步：进行聚合
                .agg(count(userActionLog("logId")).alias("actionCount"))
                // 第五步：进行排序
                .sort($"actionCount".desc)
                // 第六步：抽取指定的条数
                .limit(10)
                // 第七步：展示结果，因为监简化了，所以说不会再写入mysql
                .show()
        */

        // 功能二： 统计指定时间内购买金额最多的10个用户
        // 利用功能一构造好的 id 到 用户行为和用户基础信息的映射
        id2InfoRDD.mapToPair(tuple2 -> {
            Iterator<Tuple2<Row, Row>> iterator = tuple2._2.iterator();
            Long money = 0L;
            String username = "";
            while (iterator.hasNext()) {
                Tuple2<Row, Row> tuple = iterator.next();
                username = tuple._1.getString(1);
                money += tuple._2.getLong(4);
            }
            return new Tuple2<Long, String>(money, username);
        });
        JavaPairRDD<Long, String> money2UsernameSortRDD = id2UsernameRDD.sortByKey(false);
        List<Tuple2<Long, String>> moneyResult = money2UsernameSortRDD.take(10);
        Map<String, Long> moneyResultMap = new HashMap<>();
        for (Tuple2 tuple: result) {
            Long money = (Long) tuple._1;
            String username = (String) tuple._2;
            moneyResultMap.put(username, money);
            System.out.println(username + "购买的金额: " + money);
        }






        return null;
    }
}
