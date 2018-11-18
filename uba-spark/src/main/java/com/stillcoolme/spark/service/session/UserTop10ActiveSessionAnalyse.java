package com.stillcoolme.spark.service.session;

import com.stillcoolme.spark.dao.ISessionDetailDao;
import com.stillcoolme.spark.dao.ITop10SessionDao;
import com.stillcoolme.spark.dao.factory.DaoFactory;
import com.stillcoolme.spark.domain.SessionDetail;
import com.stillcoolme.spark.domain.Top10Session;
import com.stillcoolme.spark.service.BaseService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UserTop10ActiveSessionAnalyse {
    private final static Logger LOG = LoggerFactory.getLogger(UserTop10ActiveSessionAnalyse.class);

    /**
     * 获取每个Top10Category的点击该品类次数最高的前10个session用户
     * @param taskid
     * @param sessionId2DetailRDD
     * @param top10CategoryId 上一步骤获取的Top10category的品类ID
     */
    public static void getTop10ActiveSession(Long taskid, JavaPairRDD<String, Row> sessionId2DetailRDD, List<Long> top10CategoryId) {

        // 1.将top10热门品类的id，生成一份RDD
        List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<Tuple2<Long, Long>>();
        for(Long id : top10CategoryId) {
            top10CategoryIdList.add(new Tuple2<Long, Long>(id, id));
        }
        JavaPairRDD<Long, Long> top10CategoryIdRDD = BaseService.javaSparkContext.parallelizePairs(top10CategoryIdList);

        // 2.1 计算各品类被各session点击的次数
        JavaPairRDD<String, Iterable<Row>> sessionId2detailsRDD = sessionId2DetailRDD.groupByKey();

        JavaPairRDD<Long, String> categoryId2sessionCountRDD = sessionId2detailsRDD.flatMapToPair(
            new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Iterator<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                    String sessionId = tuple._1;
                    Iterator<Row> iterator = tuple._2.iterator();
                    Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
                    // 计算出该session，对每个品类的点击次数
                    while(iterator.hasNext()) {
                        Row row = iterator.next();
                        Long clickCategoryId = row.getAs(6);
                        if(clickCategoryId != null){
                            Long count = categoryCountMap.get(clickCategoryId);
                            if(count == null) {
                                count = 0L;
                            }
                            count++;
                            categoryCountMap.put(clickCategoryId, count);
                        }
                    }
                    // 返回的结果： <categoryId, sessionId,clickCount>
                    List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();

                    for(Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
                        long categoryid = categoryCountEntry.getKey();
                        long count = categoryCountEntry.getValue();
                        String value = sessionId + "," + count;
                        list.add(new Tuple2<Long, String>(categoryid, value));
                    }
                    return list.iterator();
                }
            }
        );
        // 2.2 计算top10品类被各session点击的次数
        JavaPairRDD<Long, String> top10CategoryId2SessionCountRDD = top10CategoryIdRDD.join(categoryId2sessionCountRDD).mapToPair(
            new PairFunction<Tuple2<Long,Tuple2<Long,String>>, Long, String>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Tuple2<Long, String> call(
                        Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
                    //LOG.warn(tuple._1 + " 点击了次数: " +tuple._2._2);
                    return new Tuple2<Long, String>(tuple._1, tuple._2._2);
                }
            }
        );

        // 3. 分组取TopN算法!! 获取top10分类的top10活跃session
        // 3.1 先按品类分组
        JavaPairRDD<Long, Iterable<String>> groupRDD = top10CategoryId2SessionCountRDD.groupByKey();
        // 3.2 每个品类的 <count,sessionid>排序
        JavaPairRDD<String, String> top10SessionRDD = groupRDD.flatMapToPair(
            new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Iterator<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
                    Long categoryId = tuple._1;
                    Iterator<String> iterator = tuple._2.iterator();
                    // 定义取topn的排序数组, 我去，存 sessionId,clickCount 字符串。然后下面每次比较就要切一下字符串
                    String[] top10Sessions = new String[10];
                    Map<String, Long> count2sessionIdMap = new HashMap();
                    while(iterator.hasNext()){
                        String sessionId2Count = iterator.next();
                        String sessionId = sessionId2Count.split(",")[0];
                        long count = Long.valueOf(sessionId2Count.split(",")[1]);
                        // 一开始是把count作为key来排序，结果翻车了，每次put入相同count的就覆盖了啊！所以要用sessionId作为key。
                        count2sessionIdMap.put(sessionId, count);
                    }

                    Map<String, Long> resultMap = count2sessionIdMap.entrySet().stream()
                            .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                    (oldValue, newValue) -> oldValue, LinkedHashMap::new));

                    LOG.warn("获取点击了品类ID: " + categoryId + "的top10个session");
                    LOG.warn("sessionId , clickCount (排序后）");
                    for(Map.Entry<String, Long> entry: resultMap.entrySet()){
                        LOG.warn(entry.getKey() + " : " +  entry.getValue());
                    }

                    // 将数据写入MySQL表
                    List<Top10Session> top10SessionList = new ArrayList();
                    List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

                    Iterator iterator1 = resultMap.keySet().iterator();
                    int top10 = 0;
                    while (iterator1.hasNext() && top10++ < 10){
                        String sessionId = (String) iterator1.next();
                        Long clickCount = resultMap.get(sessionId);
                        // 将top10 session插入MySQL表
                        Top10Session top10Session = new Top10Session();
                        top10Session.setTaskId(taskid);
                        top10Session.setCategoryId(categoryId);
                        top10Session.setSessionId(sessionId);
                        top10Session.setClickCount(clickCount);
                        top10SessionList.add(top10Session);
                        // 放入list
                        //list.add(new Tuple2<String, String>(sessionId, sessionId));
                        DaoFactory.getTop10SessionDao().insert(top10Session);
                    }
                    //DaoFactory.getTop10SessionDao().batchInsert(top10SessionList);
                    return list.iterator();
                }
            }
         );
        top10SessionRDD.count();

        // 4. 获取top10活跃session的明细数据，并写入MySQL
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
                top10SessionRDD.join(sessionId2DetailRDD);
        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));
                ISessionDetailDao sessionDetailDAO = DaoFactory.getSessionDetailDao();
                sessionDetailDAO.insert(sessionDetail);
            }
        });

    }
}
