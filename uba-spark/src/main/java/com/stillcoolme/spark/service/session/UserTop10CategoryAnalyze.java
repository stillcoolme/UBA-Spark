package com.stillcoolme.spark.service.session;

import com.stillcoolme.spark.constant.Constants;
import com.stillcoolme.spark.dao.factory.DaoFactory;
import com.stillcoolme.spark.domain.Top10Category;
import com.stillcoolme.spark.utils.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UserTop10CategoryAnalyze {

    private final static Logger LOGGER = LoggerFactory.getLogger(UserTop10CategoryAnalyze.class);

    /**
     * 获取top10热门品类
     *
     * @param filteredSessionid2AggrInfoRDD
     * @param sessionid2actionRDD
     */
    public static void getTop10Category(Long taskId, JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD, JavaPairRDD<String, Row> sessionid2actionRDD) {
        // 1. 获取符合条件的session访问明细
        JavaPairRDD<String, Tuple2<String, Row>> rdd = filteredSessionid2AggrInfoRDD.join(sessionid2actionRDD);
        JavaPairRDD<String, Row> sessionid2detailRDD = rdd.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        return new Tuple2<String, Row>(tuple._1, tuple._2._2);
                    }
                }
        );

        // 2. 获取session访问过(点击过、下单过、支付过)的所有品类id
        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        Long clickCategoryId = row.getAs(6);
                        if (clickCategoryId != null) {
                            list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
                        }
                        String orderCategoryIds = row.getString(8);
                        if (orderCategoryIds != null) {
                            String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                            for (String orderCategoryId : orderCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),
                                        Long.valueOf(orderCategoryId)));
                            }
                        }

                        String payCategoryIds = row.getString(10);
                        if (payCategoryIds != null) {
                            String[] payCategoryIdsSplited = payCategoryIds.split(",");
                            for (String payCategoryId : payCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
                                        Long.valueOf(payCategoryId)));
                            }
                        }
                        return list.iterator();
                    }
                }
        );
        // 需要去重！！！
        categoryidRDD = categoryidRDD.distinct();

        // 2.计算各品类的点击、下单和支付的次数。不是用上面的categoryidRDD来算的。。而是通过访问明细来
        // 先对访问明细数据过滤出 点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算
        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2detailRDD);
        // 计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2detailRDD);
        // 计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD);

        // 3.join各品类与它的点击、下单和支付的次数; 连接品类RDD与数据RDD ??为什么要join??
        JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD);

        // 4. 自定义二次排序key
        // 5. 将数据映射成<CategorySortKey,count>格式的RDD，然后进行二次排序（降序）
        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
                        String countInfo = tuple._2;
                        long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.REGEX_SPLIT, Constants.FIELD_CLICK_COUNT));
                        long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.REGEX_SPLIT, Constants.FIELD_ORDER_COUNT));
                        long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.REGEX_SPLIT, Constants.FIELD_PAY_COUNT));
                        CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);
                        return new Tuple2<>(sortKey, countInfo);
                    }
                }
        );
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);

        // 6. 用take(10)取出top10热门品类，并写入MySQL
        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);
        List<Top10Category> top10Categories = new ArrayList<Top10Category>();
        for (Tuple2<CategorySortKey, String> tuple2 : top10CategoryList) {
            String countInfo = tuple2._2;
            Long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.REGEX_SPLIT, Constants.FIELD_CATEGORY_ID));
            Long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.REGEX_SPLIT, Constants.FIELD_CLICK_COUNT));
            Long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.REGEX_SPLIT, Constants.FIELD_ORDER_COUNT));
            Long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, Constants.REGEX_SPLIT, Constants.FIELD_PAY_COUNT));
            Top10Category top10Category = new Top10Category();
            top10Category.set(taskId, categoryId, clickCount, orderCount, payCount);
            top10Categories.add(top10Category);
        }
        DaoFactory.getTop10CategoryDao().batchInsert(top10Categories);

    }

    // 连接品类RDD与数据RDD
    // categoryidRDD: <categoryid, categoryid> 、 clickCategoryId2CountRDD : <clickCategoryId, clickCategoryIdCount>
    private static JavaPairRDD<Long, String> joinCategoryAndData(JavaPairRDD<Long, Long> categoryidRDD,
                                                                 JavaPairRDD<Long, Long> clickCategoryId2CountRDD, JavaPairRDD<Long, Long> orderCategoryId2CountRDD, JavaPairRDD<Long, Long> payCategoryId2CountRDD) {

        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);
        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {
                        Long categoryId = tuple._1;
                        Optional<Long> optional = tuple._2._2;
                        long clickCount = 0L;
                        if (optional.isPresent()) {
                            clickCount = optional.get();
                        }
                        String value = Constants.FIELD_CATEGORY_ID + Constants.REGEX_EQUAL + categoryId + "|" + Constants.FIELD_CLICK_COUNT + Constants.REGEX_EQUAL + clickCount;
                        return new Tuple2<Long, String>(categoryId, value);
                    }
                }
        );

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                        Long categoryid = tuple._1;
                        String value = tuple._2._1;
                        Optional<Long> optional = tuple._2._2;

                        long orderCount = 0L;
                        if (optional.isPresent()) {
                            orderCount = optional.get();
                        }
                        value = value + "|" + Constants.FIELD_ORDER_COUNT + Constants.REGEX_EQUAL + orderCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }
                }
        );

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        String value = tuple._2._1;

                        Optional<Long> optional = tuple._2._2;
                        long payCount = 0L;
                        if (optional.isPresent()) {
                            payCount = optional.get();
                        }
                        value = value + "|" + Constants.FIELD_PAY_COUNT + Constants.REGEX_EQUAL + payCount;
                        return new Tuple2<Long, String>(categoryid, value);
                    }
                }
        );

        return tmpMapRDD;

    }

    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(

                new Function<Tuple2<String, Row>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(10) != null ? true : false;
                    }

                });

        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<Tuple2<Long, Long>> call(
                            Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        String payCategoryIds = row.getString(10);
                        String[] payCategoryIdsSplited = payCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        for (String payCategoryId : payCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
                        }

                        return list.iterator();
                    }

                });

        JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });

        return payCategoryId2CountRDD;
    }

    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(

                new Function<Tuple2<String, Row>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(8) != null ? true : false;
                    }

                });

        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<Tuple2<Long, Long>> call(
                            Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        String orderCategoryIds = row.getString(8);
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        for (String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
                        }

                        return list.iterator();
                    }

                });

        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });

        return orderCategoryId2CountRDD;
    }

    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                return row.getAs(6) != null ? true : false;
            }
        });
        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                long clickCategoryId = tuple._2.getAs(6);
                return new Tuple2<Long, Long>(clickCategoryId, 1L);
            }
        });
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return clickCategoryId2CountRDD;
    }
}
