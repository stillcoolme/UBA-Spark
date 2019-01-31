package com.stillcoolme.spark.service.activeuser;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stillcoolme.spark.domain.Task;
import com.stillcoolme.spark.entity.ReqEntity;
import com.stillcoolme.spark.entity.RespEntity;
import com.stillcoolme.spark.service.BaseService;
import com.stillcoolme.spark.utils.CalanderUtils;
import com.stillcoolme.spark.utils.DateUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import static org.apache.spark.sql.functions.*;
//import static org.apache.spark.sql.expressions.javalang.typed.sum;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 用户活跃度分析
 * 相关dataset操作参考：https://spark.apache.org/docs/2.4.0/api/java/org/apache/spark/sql/Dataset.html
 *
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
        String cycle = taskParam.getString("cycle");    // 用于功能3

        // 获取两份数据集
        Dataset<Row> userBaseInfo = sparkSession.read().format("json").load("uba-spark/src/main/resources/data/user_base_info.json");
        Dataset<Row> userActionLog = sparkSession.read().format("json").load("uba-spark/src/main/resources/data/user_action_log.json");
        userBaseInfo.printSchema();
        userActionLog.printSchema();
//        userActionLog.show();

        // 第一个功能：统计指定时间范围内的访问次数最多的10个用户
        Dataset<Row> userActionLogDS = userActionLog.filter("actionTime > '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 0");

        // java要先转成RDD！
        JavaRDD<Row> userActionLogRDD = userActionLogDS.toJavaRDD();
        JavaRDD<Row> userBaseInfoRDD = userBaseInfo.toJavaRDD();
        JavaPairRDD<Long, Row> userActionLogPairRDD = userActionLogRDD.mapToPair(row -> {
            return new Tuple2(row.getAs(4), row);
        });
        JavaPairRDD<Long, Row> userBaseInfoPairRDD = userBaseInfoRDD.mapToPair(row -> {
            return new Tuple2<>(row.getAs(1), row);
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
                    username = tuple._1.getString(2);
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
         * 3. Dataset编程：直接使用Dataset的转换算子。
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

        // 功能三：统计最近一个周期相比上一个周期访问次数增长最多的10个用户
        // 解决思路：上一周期的每次访问设为 -1， 现在的这一周期的每次访问设为 +1。然后相同用户的相加，就得到这一周期的用户访问相比上一周期多出多少次。
        String firstPeriodBegin = CalanderUtils.getLastCycle(CalanderUtils.MONTH_CYCLE, -1);
        String firstPeriodEnd = CalanderUtils.getLastCycle(CalanderUtils.MONTH_CYCLE, 0);
        String now = DateUtils.formatDateTime(new Date());

        // Dataset<Row> 转 DataSet<UserActionLog> : https://spark.apache.org/docs/2.4.0/api/java/org/apache/spark/sql/Dataset.html
        Dataset<UserActionLogVO> userActionLogInFirstPeriod = userActionLog.as(Encoders.bean(UserActionLog.class))
                .filter("actionTime > '" + firstPeriodBegin + "' and actionTime <= '" + firstPeriodEnd + "' and actionType = 0")
                // map一直提示不支持，一看api，原来是要这样写 Encoders在后面，然后还要添加一个转换在前面才能构造UserActionLogVO，java真难写啊。
                .map((MapFunction<UserActionLog, UserActionLogVO>) userActionLogEntry -> {
                    return  new UserActionLogVO(userActionLogEntry.getLogId(), userActionLogEntry.getUserId(), -1L);
                }, Encoders.bean(UserActionLogVO.class));

        Dataset<UserActionLogVO> userActionLogInSecondPeriod = userActionLog.as(Encoders.bean(UserActionLog.class))
                .filter("actionTime > '" + firstPeriodEnd + "' and actionTime <= '" + now + "' and actionType = 0")
                .map((MapFunction<UserActionLog, UserActionLogVO>) userActionLogEntry -> {
                    return new UserActionLogVO(userActionLogEntry.getLogId(), userActionLogEntry.getUserId(), 1L);
                }, Encoders.bean(UserActionLogVO.class));

        Dataset<UserActionLogVO> userActionLogVo = userActionLogInFirstPeriod.union(userActionLogInSecondPeriod);

        // 前两个功能没有看api，将dataset转成rdd来做，画蛇添足了。
        userActionLogVo.join(userBaseInfo, userActionLogVo.col("userId").equalTo(userBaseInfo.col("userId")))
                .groupBy(userBaseInfo.col("userId"), userBaseInfo.col("username"))
                // 点进去agg的方法描述就知道这样写了
                .agg(sum("actionValue").alias("actionIncr"))
                .sort(col("actionIncr").desc())
                .limit(10)
                .show();


        // 功能四 最近周期内相对之前一个周期购买商品金额增长最快的10个用户。
        Dataset<UserActionLogMoneyVO> aa = userActionLog.as(Encoders.bean(UserActionLog.class))
                .filter("actionTime > '" + firstPeriodBegin + "' and actionTime <= '" + firstPeriodEnd + "' and actionType = 0")
                // map一直提示不支持，一看api，原来是要这样写 Encoders在后面，然后还要添加一个转换在前面才能构造UserActionLogVO，java真难写啊。
                .map((MapFunction<UserActionLog, UserActionLogMoneyVO>) userActionLogEntry -> {
                    return  new UserActionLogMoneyVO(userActionLogEntry.getLogId(), userActionLogEntry.getUserId(), userActionLogEntry.getPurchaseMoney());
                }, Encoders.bean(UserActionLogMoneyVO.class));
        Dataset<UserActionLogMoneyVO> bb = userActionLog.as(Encoders.bean(UserActionLog.class))
                .filter("actionTime > '" + firstPeriodEnd + "' and actionTime <= '" + now + "' and actionType = 0")
                .map((MapFunction<UserActionLog, UserActionLogMoneyVO>) userActionLogEntry -> {
                    return new UserActionLogMoneyVO(userActionLogEntry.getLogId(), userActionLogEntry.getUserId(), userActionLogEntry.getPurchaseMoney());
                }, Encoders.bean(UserActionLogMoneyVO.class));
        Dataset<UserActionLogMoneyVO> userActionLogMoneyVo = aa.union(bb);

        userActionLogMoneyVo.join(userBaseInfo, userActionLogMoneyVo.col("userId").equalTo(userBaseInfo.col("userId")))
                .groupBy(userBaseInfo.col("userId"), userBaseInfo.col("username"))
                .agg(round(sum("purchaseMoney"), 2).alias("purchaseMoneyIncr"))
                .sort(col("purchaseMoneyIncr").desc())
                .limit(10)
                .show();

        // 统计指定注册时间范围内，每个用户注册头7天访问次数最高的10个用户
        userActionLogDS.join(userBaseInfo, userActionLogDS.col("userId").equalTo(userBaseInfo.col("userId")))
                .filter(userBaseInfo.col("registTime").gt(startDate).and(userBaseInfo.col("registTime").leq(endDate)))
                .filter(userActionLogDS.col("actionTime").leq(date_add(userBaseInfo.col("registTime"), 7)))
                .groupBy(userBaseInfo.col("userId"), userBaseInfo.col("username"))
                .agg(count("logId").alias("actionCount"))
                .sort(col("actionCount"))
                .limit(10)
                .show();

        return null;
    }


    public static class UserActionLogMoneyVO implements Serializable {
        Long logId;
        Long userId;
        Double purchaseMoney;


        public UserActionLogMoneyVO(Long logId, Long userId, Double purchaseMoney) {
            this.logId = logId;
            this.userId = userId;
            this.purchaseMoney = purchaseMoney;
        }

        public Long getLogId() {
            return logId;
        }

        public void setLogId(Long logId) {
            this.logId = logId;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public Double getPurchaseMoney() {
            return purchaseMoney;
        }

        public void setPurchaseMoney(Double purchaseMoney) {
            this.purchaseMoney = purchaseMoney;
        }
    }

    public static class UserActionLog implements Serializable {
        Long logId;
        Long userId;
        String actionTime;
        Long actionType;
        Double purchaseMoney;

        public Long getLogId() {
            return logId;
        }

        public void setLogId(Long logId) {
            this.logId = logId;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public String getActionTime() {
            return actionTime;
        }

        public void setActionTime(String actionTime) {
            this.actionTime = actionTime;
        }

        public Long getActionType() {
            return actionType;
        }

        public void setActionType(Long actionType) {
            this.actionType = actionType;
        }

        public Double getPurchaseMoney() {
            return purchaseMoney;
        }

        public void setPurchaseMoney(Double purchaseMoney) {
            this.purchaseMoney = purchaseMoney;
        }

    }

    public static class UserActionLogVO implements Serializable {
        Long logId;
        Long userId;
        Long actionValue;


        public UserActionLogVO(Long logId, Long userId, Long actionValue) {
            this.logId = logId;
            this.userId = userId;
            this.actionValue = actionValue;
        }

        public Long getLogId() {
            return logId;
        }

        public void setLogId(Long logId) {
            this.logId = logId;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public Long getActionValue() {
            return actionValue;
        }

        public void setActionValue(Long actionValue) {
            this.actionValue = actionValue;
        }
    }

}
