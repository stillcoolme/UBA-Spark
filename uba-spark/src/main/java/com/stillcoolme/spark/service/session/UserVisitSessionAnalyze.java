package com.stillcoolme.spark.service.session;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stillcoolme.spark.constant.Constants;
import com.stillcoolme.spark.dao.ISessionAggrStatDao;
import com.stillcoolme.spark.dao.ISessionDetailDao;
import com.stillcoolme.spark.dao.ISessionRandomExtractDao;
import com.stillcoolme.spark.domain.SessionAggrStat;
import com.stillcoolme.spark.domain.SessionDetail;
import com.stillcoolme.spark.domain.SessionRandomExtract;
import com.stillcoolme.spark.domain.Task;
import com.stillcoolme.spark.entity.ReqEntity;
import com.stillcoolme.spark.entity.RespEntity;
import com.stillcoolme.spark.dao.factory.DaoFactory;
import com.stillcoolme.spark.service.BaseService;
import com.stillcoolme.spark.utils.DateUtils;
import com.stillcoolme.spark.utils.NumberUtils;
import com.stillcoolme.spark.utils.ParamUtils;
import com.stillcoolme.spark.utils.StringUtils;
import com.stillcoolme.spark.utils.ValidUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.AccumulatorV2;
import org.glassfish.jersey.server.Broadcaster;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 用户访问的 session 分析Spark作业， 主要使用了 mapToPair 聚合 过滤
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 * 我们的spark作业如何接受用户创建的任务？
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param字段中
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 *
 * Created by zhangjianhua on 2018/10/30.
 */
public class UserVisitSessionAnalyze extends BaseService {
    private final static Logger logger = Logger.getLogger(UserVisitSessionAnalyze.class);

    @Override
    public RespEntity run(ReqEntity reqEntity) {

        RespEntity resEntity = new RespEntity();

        // 基本就是 sparkAgent的写法了 ！！
        JSONArray jsonArray = JSONArray.parseArray(reqEntity.reqData);
        JSONObject lstCondition = jsonArray.getJSONObject(0);

        // 客户端 submit 传过来 任务taskId，通过taskId去mysql查询出来指定的任务，并获取任务的查询参数
        Long taskid = lstCondition.getLong("taskId");
        if (taskid == null) {
            return null;
        }
        Task task = taskDao.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 进行session粒度的数据聚合,首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(taskParam);
        // <session, visitActionInfo>
        JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);
        sessionid2ActionRDD = sessionid2ActionRDD.persist(StorageLevel.MEMORY_ONLY());

        // 将行为数据根据session_id进行groupByKey分组(session粒度)，然后与用户信息数据进行join就可以获取到session粒度的操作及用户数据
        // 获取的数据变成<sessionid,(sessionid,searchKeywords,clickCategoryIds,visitlength, steplength, age,professional,city,sex)>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sessionid2ActionRDD);

        logger.warn("过滤前: " + sessionid2AggrInfoRDD.count());
        for (Tuple2<String, String> tuple : sessionid2AggrInfoRDD.take(1)) {
            logger.warn(tuple._2);
        }
        // 注册Accumulator
        AccumulatorV2<String, String> sessionAggrStatAccumulator = new UserVisitSessionAccumulator();
        sparkSession.sparkContext().register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator");
        // 针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤， 然后再通过Accumulator聚合统计
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

        // 将随机抽取的功能放在session聚合统计功能的最终计算和写库之前, 该方法里有一个countByKey算子，是action操作，会触发job
        randomExtractSession(taskid, filteredSessionid2AggrInfoRDD, sessionid2ActionRDD, 20);

//        logger.warn("过滤后: " + filteredSessionid2AggrInfoRDD.count());
//        for (Tuple2<String, String> tuple : filteredSessionid2AggrInfoRDD.take(10)) {
//            logger.warn(tuple._2());
//        }
        // 在这之前有多少个 action算子，这个Accumulator就会多运算几次，结果就会出错，所以一般是在插入mysql前执行action。
        logger.warn(sessionAggrStatAccumulator.value());
        //计算出各个范围的session占比，并连同聚合信息一起写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskid());

        // 经过筛选的session的明细数据，可否放在随机抽取前面？给随机抽取方法用，这样随机抽取就不用遍历这么多了。
        JavaPairRDD<String, Row> sessionId2DetailRDD = getFilterSessionid2AggrInfoRDD(filteredSessionid2AggrInfoRDD, sessionid2ActionRDD);
        sessionid2ActionRDD = sessionId2DetailRDD.persist(StorageLevel.MEMORY_ONLY());

        // 获取点击top10的品类信息
        List<Long> top10Ids = UserTop10CategoryAnalyze.getTop10Category(taskid, sessionId2DetailRDD, sessionid2ActionRDD);
        // 获取top10的session
        UserTop10ActiveSessionAnalyse.getTop10ActiveSession(taskid, sessionId2DetailRDD, top10Ids);

        return null;
    }


    /**
     * 获取sessionid到访问行为数据的映射的RDD
     * @param actionRDD
     * @return
     */
    public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
//        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
//            private static final long serialVersionUID = 1L;
//            @Override
//            public Tuple2<String, Row> call(Row row) throws Exception {
//                // row.getString(2) 得到 sessionId
//                return new Tuple2<String, Row>(row.getString(2), row);
//            }
//        });
        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<Tuple2<String, Row>> call(Iterator<Row> rowIterator) throws Exception {
                List<Tuple2<String, Row>> list = new ArrayList();
                while(rowIterator.hasNext()){
                    Row row = rowIterator.next();
                    list.add(new Tuple2<>(row.getString(2), row));
                }
                return list.iterator();
            }
        });
    }

    /**
     * 得到 通过筛选条件的session的访问明细数据RDD
     * @param filteredSessionid2AggrInfoRDD ：过滤后的 RDD
     * @param sessionid2ActionRDD ： sessionId的访问行为数据
     * @return
     */
    public JavaPairRDD<String, Row> getFilterSessionid2AggrInfoRDD(
            JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2ActionRDD){
        JavaPairRDD<String, Tuple2<String, Row>> rdd = (JavaPairRDD<String, Tuple2<String, Row>>) filteredSessionid2AggrInfoRDD.join(sessionid2ActionRDD);
//        JavaPairRDD<String, Row> sessionid2detailRDD = rdd.mapToPair(
//                new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
//                    private static final long serialVersionUID = 1L;
//                    @Override
//                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//                        return new Tuple2<String, Row>(tuple._1, tuple._2._2);
//                    }
//                }
//        );
        JavaPairRDD<String, Row> sessionid2detailRDD = rdd.mapPartitionsToPair(
            new PairFlatMapFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>, String, Row>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Iterator<Tuple2<String, Row>> call(Iterator<Tuple2<String, Tuple2<String, Row>>> tuple2Iterator) throws Exception {
                    List<Tuple2<String, Row>> list = new ArrayList<>();
                    while (tuple2Iterator.hasNext()){
                        Tuple2<String, Tuple2<String, Row>> tuple = tuple2Iterator.next();
                        list.add(new Tuple2<String, Row>(tuple._1, tuple._2._2));
                    }
                    return list.iterator();
                }
            }
        );
        return sessionid2detailRDD;
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     *
     * @param taskParam 任务参数
     * @return 行为数据RDD
     */
    public static JavaRDD<Row> getActionRDDByDateRange(JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql =
                "select * "
                        + "from user_visit_action "
                        + "where date>='" + startDate + "' "
                        + "and date<='" + endDate + "'";

        Dataset<Row> actionDF = sparkSession.sql(sql);
        // repartition 解决 Spark SQL可能出现低并行度的性能问题
        return actionDF.javaRDD().repartition(20);
    }

    /**
     * 对行为数据按session粒度进行聚合
     *
     * @param sessionid2ActionRDD <sessionId, 行为数据>RDD
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaPairRDD<String, Row> sessionid2ActionRDD) {
        // actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索，将这个Row映射成<sessionid,Row>的格式
//        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(
//                new PairFunction<Row, String, Row>() {
//                    private static final long serialVersionUID = 1L;
//                    @Override
//                    public Tuple2<String, Row> call(Row row) throws Exception {
//                        return new Tuple2<String, Row>(row.getString(2), row);
//                    }
//
//                });

        // 对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD =
                sessionid2ActionRDD.groupByKey();

        // 对每组session聚合，将session中所有的搜索词和点击品类都聚合起来 得到<userid, partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)
                            throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                        Long userid = null;

                        // session的访问开始时间、结束时间 和访问步长
                        Date startTime = null;
                        Date endTime = null;
                        int stepLength = 0;

                        // 遍历session所有的访问行为
                        while (iterator.hasNext()) {
                            // 提取每个访问行为的搜索词字段和点击品类字段
                            Row row = iterator.next();
                            if (userid == null) {
                                userid = row.getLong(1);
                            }

                            String searchKeyword = row.getString(5);
                            Long clickCategoryId = row.getAs(6);

                            // 实际上这里要对数据说明一下
                            // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                            // (只有搜索行为，是有searchKeyword字段的；只有点击品类的行为，是有clickCategoryId字段）
                            // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的
                            // 我们决定是否将搜索词或点击品类id拼接到字符串中去
                            // 首先要满足：不能是null值；其次，之前的字符串中还没有搜索词或者点击品类id
                            if (StringUtils.isNotEmpty(searchKeyword)) {
                                if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }
                            if (clickCategoryId != null) {
                                if (!clickCategoryIdsBuffer.toString().contains(
                                        String.valueOf(clickCategoryId))) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            //计算session的访问开始结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));
                            if (startTime == null) {
                                startTime = actionTime;
                            }
                            if (endTime == null) {
                                endTime = actionTime;
                            }

                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if (actionTime.after(endTime)) {
                                endTime = actionTime;
                            }
                            // 计算session访问步长
                            stepLength++;

                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                        // 计算session访问时长（秒）
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        // 思考一下
                        // 我们返回的数据格式，即使<sessionid,partAggrInfo>
                        // 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
                        // 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
                        // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
                        // 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
                        // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

                        // 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
                        // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
                        // 然后再直接将返回的Tuple的key设置成sessionid
                        // 最后的数据格式，还是<sessionid,fullAggrInfo>

                        // 聚合数据，用什么样的格式进行拼接？
                        // 我们这里统一定义，使用key=value|key=value
                        String partAggrInfo = Constants.FIELD_SESSION_ID + Constants.REGEX_EQUAL + sessionid + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + Constants.REGEX_EQUAL + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + Constants.REGEX_EQUAL + clickCategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + Constants.REGEX_EQUAL + visitLength + "|"
                                + Constants.FIELD_STEP_LENGTH + Constants.REGEX_EQUAL + stepLength + "|"
                                + Constants.FIELD_START_TIME + Constants.REGEX_EQUAL + DateUtils.formatTime(startTime);

                        return new Tuple2<Long, String>(userid, partAggrInfo);
                    }

                });

        // 查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sparkSession.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(

                new PairFunction<Row, Long, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        return new Tuple2<Long, Row>(row.getLong(0), row);
                    }

                });

        // 将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD =
                (JavaPairRDD<Long, Tuple2<String, Row>>) userid2PartAggrInfoRDD.join(userid2InfoRDD);

        // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(

                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(
                            Tuple2<Long, Tuple2<String, Row>> tuple)
                            throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionid = StringUtils.getFieldFromConcatString(
                                partAggrInfo, Constants.REGEX_SPLIT, Constants.FIELD_SESSION_ID);

                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo + "|"
                                + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex;

                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
                    }

                });

        return sessionid2FullAggrInfoRDD;
    }

    /**
     * 过滤session数据 并 进行聚合统计
     * @param sessionid2AggrInfoRDD
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final AccumulatorV2<String, String> sessionAggrStatAccumulator) {
        // 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
        // 此外，这里其实大家不要觉得是多此一举
        // 其实我们是给后面的性能优化埋下了一个伏笔
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

        if (_parameter.endsWith(Constants.REGEX_SPLIT)) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(

                new Function<Tuple2<String, String>, Boolean>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        // 首先，从tuple中，获取聚合数据
                        String aggrInfo = tuple._2;

                        // 接着，依次按照筛选条件进行过滤
                        // 按照年龄范围进行过滤（startAge、endAge）
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        // 按照职业范围进行过滤（professionals）
                        // 互联网,IT,软件
                        // 互联网
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        // 按照城市范围进行过滤（cities）
                        // 北京,上海,广州,深圳
                        // 成都
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                                parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        // 按照性别进行过滤
                        // 男/女
                        // 男，女
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                                parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 按照搜索词进行过滤
                        // 我们的session可能搜索了 火锅,蛋糕,烧烤
                        // 我们的筛选条件可能是 火锅,串串香,iphone手机
                        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                        // 任何一个搜索词相当，即通过
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                                parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        // 按照点击品类id进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                                parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        // 如果经过了之前的多个过滤条件之后，程序能够走到这里
                        // 那么就说明，该session是符合用户指定的筛选条件，也就是需要保留的session
                        // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
                        // 进行相应的累加计数
                        // 取出session中的 visitLength、steplength
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, Constants.REGEX_SPLIT, Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, Constants.REGEX_SPLIT, Constants.FIELD_STEP_LENGTH));
                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);
                        calculateSession();
                        return true;
                    }

                    /**
                     * 计算有多少个session通过了筛选
                     */
                    private void calculateSession() {
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                    }

                    /**
                     * 计算访问时长范围
                     * @param visitLength
                     */
                    private void calculateVisitLength(long visitLength) {
                        if (visitLength > 0 && visitLength < 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 计算访问步长范围
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }
                });
        return filteredSessionid2AggrInfoRDD;
    }



    private void calculateAndPersistAggrStat(String value, long taskid) {
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.SESSION_COUNT));
        if(session_count <= 0){
            return;
        }
        Double visit_length_1s_3s = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.TIME_PERIOD_1s_3s));
        Double visit_length_4s_6s = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.TIME_PERIOD_4s_6s));
        Double visit_length_7s_9s = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.TIME_PERIOD_7s_9s));
        Double visit_length_10s_30s = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.TIME_PERIOD_10s_30s));
        Double visit_length_30s_60s = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.TIME_PERIOD_30s_60s));
        Double visit_length_1m_3m = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.TIME_PERIOD_1m_3m));
        Double visit_length_3m_10m = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.TIME_PERIOD_3m_10m));
        Double visit_length_10m_30m = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.TIME_PERIOD_10m_30m));
        Double visit_length_30m = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.TIME_PERIOD_30m));

        Double step_length_1_3 = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.STEP_PERIOD_1_3));
        Double step_length_4_6 = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.STEP_PERIOD_4_6));
        Double step_length_7_9 = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.STEP_PERIOD_7_9));
        Double step_length_10_30 = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.STEP_PERIOD_10_30));
        Double step_length_30_60 = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.STEP_PERIOD_30_60));
        Double step_length_60 = Double.valueOf(StringUtils.getFieldFromConcatString(value, Constants.REGEX_SPLIT, Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的Dao插入统计结果
        ISessionAggrStatDao sessionAggrStatDao = DaoFactory.getSessionAggrStatDao();
        sessionAggrStatDao.insert(sessionAggrStat);
    }


    /**
     * 从过滤好的聚合信息中 按时间比例抽取出sesison
     * @param taskid
     * @param sessionid2AggrInfoRDD 聚合信息RDD
     * @param sessionid2actionRDD 用户完整行为信息RDD
     * @param extractNum 要抽取的session总数
     */
    private static void randomExtractSession(final Long taskid,
                                             JavaPairRDD<String, String> sessionid2AggrInfoRDD,
                                             JavaPairRDD<String, Row> sessionid2actionRDD,
                                             final int extractNum) {
        // 1.计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD，可不是<yyyy-MM-dd_HH,sessionId>
        JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<String,String>, String, String>(){
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                        String aggrInfo = tuple._2;
                        String startTime = StringUtils.getFieldFromConcatString(
                                aggrInfo, Constants.REGEX_SPLIT, Constants.FIELD_START_TIME);
                        String dateHour = DateUtils.getDateHour(startTime);
                        return new Tuple2(dateHour, aggrInfo);
                    }
                }
        );

        // 每小时session数量的map
        Map<String, Long> countMap = time2sessionidRDD.countByKey();
        // 总的session数量
        Long sessionSum = 0L;
        for(String key : countMap.keySet()){
            sessionSum += countMap.get(key);
        }
        // 每小时session的需要插取的个数。
        Map<String, Long> session2Extract = new HashMap<>();
        // 存放每小时session的需要插取的随机抽区索引。 <hour，list< 2,4,5>>  表示该hour下要抽取索引为 2，4，5的session。
        Map<String, List> session2extractlistMap = new HashMap<>();
        Random random = new Random();
        for(String key: countMap.keySet()){
            // key的格式是 yyyy-MM-dd_HH
            long sesionCount = countMap.get(key);
            // 每个小时需要抽取的session数量
            int eachHourextractNum = (int) ((double)sesionCount / (double)sessionSum * extractNum);
            if(eachHourextractNum > sesionCount){
                eachHourextractNum = (int) sesionCount;
            }
            List extractIndexList = new ArrayList();
            // 生成上面计算出来的数量的随机数
            for(int i = 0; i < eachHourextractNum; i++) {
                int extractIndex = random.nextInt((int) sesionCount);
                while(extractIndexList.contains(extractIndex)) {
                    extractIndex = random.nextInt((int) sesionCount);
                }
                extractIndexList.add(extractIndex);
            }
            session2extractlistMap.put(key, extractIndexList);

        }

        // 执行groupByKey算子，得到 <dateHour, list(aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();
        // 广播大变量
        final Broadcast<Map<String, List>> broadcastVar = javaSparkContext.broadcast(session2extractlistMap);

        // 3. flatMap算子遍历每小时的session数据，<dateHour, list(aggrInfo)>格式
        // 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上则抽取该session，直接写入MySQL的random_extract_session表
        JavaPairRDD<String, String> extractSessionidsRDD =  time2sessionsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Iterator<Tuple2<String, String>> call(
                            Tuple2<String, Iterable<String>> tuple) throws Exception {
                        // yyyy-MM-DD_hh
                        String datehour = tuple._1;
                        // 使用广播变量
                        Map<String, List> broadcastMap = broadcastVar.value();
                        List extractIndexList = broadcastMap.get(datehour);
                        Iterator<String> iterator = tuple._2.iterator();
                        List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();
                        ISessionRandomExtractDao sessionRandomExtractDao = DaoFactory.getSessionRandomExtractDao();
                        int index  = 0;
                        for(String sessionAggrInfo: tuple._2){
                            //String sessionAggrInfo = iterator.next();
                            if(extractIndexList.contains(index)){
                                // 构建SessionRandomExtract
                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                                String sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, Constants.REGEX_SPLIT, Constants.FIELD_SESSION_ID);
                                String startTime = StringUtils.getFieldFromConcatString(sessionAggrInfo, Constants.REGEX_SPLIT, Constants.FIELD_START_TIME);
                                String searchKeyWords = StringUtils.getFieldFromConcatString(sessionAggrInfo, Constants.REGEX_SPLIT, Constants.FIELD_SEARCH_KEYWORDS);
                                String clickCategoryIds = StringUtils.getFieldFromConcatString(sessionAggrInfo, Constants.REGEX_SPLIT, Constants.FIELD_CLICK_CATEGORY_IDS);
                                sessionRandomExtract.set(taskid, sessionid, startTime, searchKeyWords, clickCategoryIds);
                                sessionRandomExtractDao.insert(sessionRandomExtract);
                                // 将sessionid加入list
                                extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
                            }
                            index++;
                        }
                        return extractSessionids.iterator();
                    }
                });

        // 4. 抽取出来的session id 和 sessionAction数据 join 得到 detail数据，然后入库
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                (JavaPairRDD<String, Tuple2<String, Row>>) extractSessionidsRDD.join(sessionid2actionRDD);
//        extractSessionDetailRDD.foreach(
//                new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {
//                    private static final long serialVersionUID = 1L;
//                    @Override
//                    public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//                        Row row = tuple._2._2;
//                        SessionDetail sessionDetail = new SessionDetail();
//                        sessionDetail.setTaskid(taskid);
//                        if(row.getAs(1) != null){
//                            sessionDetail.setUserid(row.getAs(1));
//                        }
//                        if(row.getAs(3) != null){
//                            sessionDetail.setPageid(row.getAs(3));
//                        }
//                        sessionDetail.setActionTime(row.getString(4));
//                        sessionDetail.setSearchKeyword(row.getString(5));
//                        if(row.getAs(6) != null){
//                            sessionDetail.setClickCategoryId(row.getAs(6));
//                        }
//                        if(row.getAs(7) != null){
//                            sessionDetail.setClickProductId(row.getAs(7));
//                        }
//                        sessionDetail.setOrderCategoryIds(row.getString(8));
//                        sessionDetail.setOrderProductIds(row.getString(9));
//                        sessionDetail.setPayCategoryIds(row.getString(10));
//                        sessionDetail.setPayProductIds(row.getString(11));
//                        ISessionDetailDao sessionDetailDao = DaoFactory.getSessionDetailDao();
//                        sessionDetailDao.insert(sessionDetail);
//                    }
//                });
        try{
            extractSessionDetailRDD.foreachPartition(
                new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
                private static final long serialVersionUID = 1L;
                @Override
                public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> tuple2Iterator) throws Exception {
                    List<SessionDetail> dataList = new ArrayList();
                    while(tuple2Iterator.hasNext()){
                        Tuple2<String, Tuple2<String, Row>> tuple = tuple2Iterator.next();
                        Row row = tuple._2._2;
                        String sessionId=tuple._1;
                        SessionDetail sessionDetail = new SessionDetail();
                        sessionDetail.setSessionid(sessionId);
                        sessionDetail.setTaskid(taskid);
                        if(row.getAs(1) != null){
                            sessionDetail.setUserid(row.getAs(1));
                        }
                        if(row.getAs(3) != null){
                            sessionDetail.setPageid(row.getAs(3));
                        }
                        sessionDetail.setActionTime(row.getString(4));
                        sessionDetail.setSearchKeyword(row.getString(5));
                        if(row.getAs(6) != null){
                            sessionDetail.setClickCategoryId(row.getAs(6));
                        }
                        if(row.getAs(7) != null){
                            sessionDetail.setClickProductId(row.getAs(7));
                        }
                        sessionDetail.setOrderCategoryIds(row.getString(8));
                        sessionDetail.setOrderProductIds(row.getString(9));
                        sessionDetail.setPayCategoryIds(row.getString(10));
                        sessionDetail.setPayProductIds(row.getString(11));
                        dataList.add(sessionDetail);
                    }
                    DaoFactory.getSessionDetailDao().insertBatch(dataList);
                }
            });
        }catch (Exception e){
            logger.warn("随机抽取数据入库失败" + e.getMessage());
        }
        logger.warn("随机抽取数据入库成功");
    }

}
