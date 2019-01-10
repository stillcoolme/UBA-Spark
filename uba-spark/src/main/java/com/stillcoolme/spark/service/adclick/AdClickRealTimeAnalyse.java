package com.stillcoolme.spark.service.adclick;

import com.stillcoolme.spark.constant.Constants;
import com.stillcoolme.spark.dao.IAdClickTrendDao;
import com.stillcoolme.spark.dao.IAdProvinceTop3Dao;
import com.stillcoolme.spark.dao.IAdUserClickCountDao;
import com.stillcoolme.spark.dao.factory.DaoFactory;
import com.stillcoolme.spark.domain.AdBlacklist;
import com.stillcoolme.spark.domain.AdClickTrend;
import com.stillcoolme.spark.domain.AdProvinceTop3;
import com.stillcoolme.spark.domain.AdStat;
import com.stillcoolme.spark.domain.AdUserClickCount;
import com.stillcoolme.spark.entity.ReqEntity;
import com.stillcoolme.spark.entity.RespEntity;
import com.stillcoolme.spark.service.BaseService;
import com.stillcoolme.spark.utils.Config;
import com.stillcoolme.spark.utils.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * @author stillcoolme
 * @date 2018/12/10 21:51
 */
public class AdClickRealTimeAnalyse extends BaseService {

    @Override
    public RespEntity run(ReqEntity req) {

        // 创建针对Kafka数据来源的输入DStream（离线流，代表了一个源源不断的数据来源，抽象）
        // 选用kafka direct api（很多好处，包括自己内部自适应调整每次接收数据量的特性，等等）
        // 构建kafka参数map
        // 主要要放置的就是，你要连接的kafka集群的地址（broker集群的地址列表）
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST,
                Config.kafkaProps.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
        // 构建topic set
        String kafkaTopics = Config.kafkaProps.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");
        Set<String> topics = new HashSet<String>();
        for (String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }
        // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
        // http://spark.apache.org/docs/2.1.1/streaming-kafka-0-8-integration.html
        // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                javaStreamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);

        // 数据格式介绍：
        //timestamp	province city userid adid

        // 根据动态黑名单进行数据过滤
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream =
                filterByBlacklist(adRealTimeLogDStream);

        // 生成动态黑名单
        generateDynamicBlacklist(filteredAdRealTimeLogDStream);

        // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid, clickCount）, 粒度最粗
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(
                filteredAdRealTimeLogDStream);

        // 业务功能二：实时统计每天每个省份top3热门广告，统计的稍微细一些
        calculateProvinceTop3Ad(adRealTimeStatDStream);

        // 业务功能三：实时统计每天每个广告在最近1小时内的每分钟作为一个滑动窗口内的点击趋势，统计粒度非常细
        calculateAdClickCountByWindow(adRealTimeLogDStream);

        // 构建完spark streaming上下文之后，记得要进行上下文的启动、等待执行结束、关闭
        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        javaStreamingContext.close();
        return null;
    }

    /**
     * 实时统计每天每个广告在最近1小时内将每分钟作为一个滑动窗口内的点击趋势
     * @param adRealTimeLogDStream
     */
    private void calculateAdClickCountByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        //// timestamp province city userid adid 映射成<yyyyMMddHHMM_adid,1L>格式
        JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(tuple -> {
            String[] logSplited = tuple._2.split(" ");
            String timeMinute = DateUtils.formatTimeMinute(new Date(Long.valueOf(logSplited[0])));
            long adid = Long.valueOf(logSplited[4]);
            return new Tuple2<String, Long>(timeMinute + "_" + adid, 1L);
        });

        // 每次出来一个新的batch，都要获取最近1小时内的所有的batch
        // 然后根据key进行reduceByKey操作，统计出来最近一小时内的各分钟各广告的点击次数
        // 1小时滑动窗口内的广告点击趋势
        // 点图 / 折线图
        JavaPairDStream<String, Long> aggrRDD = pairDStream.reduceByKeyAndWindow((Long v1, Long v2) -> {
            return v1 + v2;
        }, Durations.minutes(60), Durations.seconds(10));

        // aggrRDD每次都可以拿到，最近1小时内，各分钟（yyyyMMddHHMM）各广告的点击量
        aggrRDD.foreachRDD(rdd -> {
            rdd.foreachPartition((Iterator<Tuple2<String, Long>> iterator) -> {
                List<AdClickTrend> adClickTrends = new ArrayList<AdClickTrend>();
                while(iterator.hasNext()) {
                    Tuple2<String, Long> tuple = iterator.next();
                    String[] keySplited = tuple._1.split("_");
                    // yyyyMMddHHmm
                    String dateMinute = keySplited[0];
                    long adid = Long.valueOf(keySplited[1]);
                    long clickCount = tuple._2;

                    String date = DateUtils.formatDate(DateUtils.parseDateKey(
                            dateMinute.substring(0, 8)));
                    String hour = dateMinute.substring(8, 10);
                    String minute = dateMinute.substring(10);

                    AdClickTrend adClickTrend = new AdClickTrend();
                    adClickTrend.setDate(date);
                    adClickTrend.setHour(hour);
                    adClickTrend.setMinute(minute);
                    adClickTrend.setAdid(adid);
                    adClickTrend.setClickCount(clickCount);

                    adClickTrends.add(adClickTrend);
                }
                IAdClickTrendDao adClickTrendDao = DaoFactory.getAdClickTrendDAO();
                adClickTrendDao.updateBatch(adClickTrends);
            });
        });
    }

    /**
     * 计算每天各省份的top3热门广告
     * @param adRealTimeStatDStream    （yyyyMMdd_province_city_adid, clickCount）
     */
    private void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {
        // 传入的adRealTimeStatDStream 每一个batch rdd，都代表了最新的全量的每天各省份各城市各广告的点击量
        JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform( (JavaPairRDD<String, Long> rdd) -> {
            // <yyyyMMdd_province_city_adid, clickCount> 转成 <yyyyMMdd_province_adid, clickCount>
            // 计算出每天各省份各广告的点击量
            JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(tuple -> {
                String[] keySplited = tuple._1.split("_");
                String date = keySplited[0];
                String province = keySplited[1];
                long adid = Long.valueOf(keySplited[3]);
                long clickCount = tuple._2;
                String key = date + "_" + province + "_" + adid;
                return new Tuple2<String, Long>(key, clickCount);
            });
            JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey((v1, v2) -> {return v1 + v2;});

            // 将dailyAdClickCountByProvinceRDD转换为DataFrame
            // 注册为一张临时表
            // 使用Spark SQL，通过开窗函数，获取到各省份的top3热门广告
            JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(tuple -> {
                String[] keySplited = tuple._1.split("_");
                String datekey = keySplited[0];
                String province = keySplited[1];
                long adid = Long.valueOf(keySplited[2]);
                long clickCount = tuple._2;
                String date = DateUtils.formatDate(DateUtils.parseDateKey(datekey));
                return RowFactory.create(date, province, adid, clickCount);
            });

            StructType schema = DataTypes.createStructType(Arrays.asList(
                    new StructField[]{
                            DataTypes.createStructField("date", DataTypes.StringType, true),
                            DataTypes.createStructField("province", DataTypes.StringType, true),
                            DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                            DataTypes.createStructField("click_count", DataTypes.LongType, true)
                    }
             ));
            Dataset<Row> dailyAdClickCountByProvinceDF = sparkSession.createDataFrame(rowsRDD, schema);
            // 将dailyAdClickCountByProvinceDF，注册成一张临时表
            dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");
            String sql = "SELECT date, province, ad_id, click_count FROM "
                    + "(SELECT date, province, ad_id, click_count, "
                    + "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank "
                    + "FROM tmp_daily_ad_click_count_by_prov) t"
                    + "WHERE rand >= 3";
            // 使用Spark SQL执行SQL语句，配合开窗函数，统计出各身份top3热门的广告
            Dataset provinceTop3AdDF = sparkSession.sql(sql);
            return provinceTop3AdDF.javaRDD();
        });

        // rowsDStream 每次都是刷新出来各个省份最热门的top3广告, 数据批量更新到MySQL中
        rowsDStream.foreachRDD(rdd -> {
            rdd.foreachPartition((Iterator<Row> iterator) -> {
                List<AdProvinceTop3> adProvinceTop3s = new ArrayList<AdProvinceTop3>();
                while(iterator.hasNext()) {
                    Row row = iterator.next();
                    String date = row.getString(0);
                    String province = row.getString(1);
                    long adid = row.getLong(2);
                    long clickCount = row.getLong(3);
                    AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                    adProvinceTop3.setDate(date);
                    adProvinceTop3.setProvince(province);
                    adProvinceTop3.setAdid(adid);
                    adProvinceTop3.setClickCount(clickCount);
                    adProvinceTop3s.add(adProvinceTop3);
                }
                IAdProvinceTop3Dao adProvinceTop3DAO = DaoFactory.getAdProvinceTop3Dao();
                adProvinceTop3DAO.updateBatch(adProvinceTop3s);
            });
        });
    }

    /**
     * 计算广告点击流量实时统计
     * 传入的数据 timestamp	province city userid adid
     * @param filteredAdRealTimeLogDStream
     * @return
     */
    private JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {

        // 设计出来几个维度：日期、省份、城市、广告 四个维度来展示给用户查看的
        // 对原始数据进行map，映射成<date_province_city_adid,1>格式
        // 再执行updateStateByKey算子(streaminig特有的算子，在spark集群内存中，维护一份key的全局状态)
        JavaPairDStream<String, Long> mappedDStream = filteredAdRealTimeLogDStream.mapToPair(tuple -> {
            String log = tuple._2;
            String[] logSplited = log.split(" ");
            String timestamp = logSplited[0];
            Date date = new Date(Long.valueOf(timestamp));
            String datekey = DateUtils.formatDateKey(date);	// yyyyMMdd
            String province = logSplited[1];
            String city = logSplited[2];
            long adid = Long.valueOf(logSplited[4]);
            String key = datekey + "_" + province + "_" + city + "_" + adid;
            return new Tuple2<String, Long>(key, 1L);
        });

        // 每次计算出最新的值，就在aggregatedDStream中的每个batch rdd中反应出来
        JavaPairDStream<String, Long> aggregatedDStream = mappedDStream.updateStateByKey((List<Long> values, Optional<Long> optional) -> {
            // 对于每个key，都会调用一次这个方法
            // 首先根据optional判断，之前这个key，是否有对应的状态
            long clickCount = 0L;
            // 如果之前是存在这个状态的，那么就以之前的状态作为起点，进行值的累加
            if(optional.isPresent()) {
                clickCount = optional.get();
            }
            // values代表了batch rdd中，每个key对应的所有的值
            for(Long value: values){
                clickCount += value;
            }
            return Optional.of(clickCount);
        });
        // 将计算出来的最新结果，同步一份到mysql中，以便于j2ee系统使用
        aggregatedDStream.foreachRDD(rdd -> {
            // Iterator<Tuple2<String, Long>> iterator
            rdd.foreachPartition(iterator ->{
                List<AdStat> adStats = new ArrayList<AdStat>();
                while (iterator.hasNext()){
                    Tuple2<String, Long> tuple = iterator.next();
                    String[] keySplited = tuple._1.split("_");
                    String date = keySplited[0];
                    String province = keySplited[1];
                    String city = keySplited[2];
                    long adid = Long.valueOf(keySplited[3]);
                    long clickCount = tuple._2;

                    AdStat adStat = new AdStat();
                    adStat.setDate(date);
                    adStat.setProvince(province);
                    adStat.setCity(city);
                    adStat.setAdid(adid);
                    adStat.setClickCount(clickCount);
                    adStats.add(adStat);
                }
                DaoFactory.getAdStatDao().updateBatch(adStats);
            });
        });
        return aggregatedDStream;
    }

    /**
     * 生成动态黑名单
     * @param filteredAdRealTimeLogDStream
     */
    private void generateDynamicBlacklist(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        // 实时日志处理，格式：timestamp province city userid adid （某个时间点 某个省份 某个城市 某个用户 某个广告）
        // 计算出每5个秒内的数据中，每天每个用户每个广告的点击量
        // 通过对原始实时日志的处理，将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
        JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                // 从tuple中获取到每一条原始的实时日志
                String log = tuple._2;
                String[] logSplited = log.split(" ");
                // 提取出日期（yyyyMMdd）、userid、adid
                String timestamp = logSplited[0];
                String datekey = DateUtils.formatDateKey(new Date(Long.valueOf(timestamp)));
                String userId = logSplited[3];
                String adId = logSplited[4];
                // 拼接key
                String key = datekey + "_" + userId + "_" + adId;
                return new Tuple2<String, Long>(key, 1L);
            }
        });
        // 针对处理后的日志格式，执行reduceByKey算子即可。（每个batch中）每天每个用户对每个广告的点击量<yyyyMMdd_userid_adid, clickCount>
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey((v1, v2) -> v1 + v2);

        dailyUserAdClickCountDStream.foreachRDD(rdd -> {
            rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                    // 对每个分区的数据就去获取一次连接对象
                    // 每次都是从连接池中获取，而不是每次都创建
                    // 写数据库操作，性能已经提到最高了
                    List<AdUserClickCount> adUserClickCounts = new ArrayList<AdUserClickCount>();
                    while (iterator.hasNext()) {
                        Tuple2<String, Long> tuple = iterator.next();
                        String[] keySplited = tuple._1.split("_");
                        String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                        // yyyy-MM-dd
                        long userid = Long.valueOf(keySplited[1]);
                        long adid = Long.valueOf(keySplited[2]);
                        long clickCount = tuple._2;
                        AdUserClickCount adUserClickCount = new AdUserClickCount();
                        adUserClickCount.setDate(date);
                        adUserClickCount.setUserid(userid);
                        adUserClickCount.setAdid(adid);
                        adUserClickCount.setClickCount(clickCount);

                        adUserClickCounts.add(adUserClickCount);
                    }
                    IAdUserClickCountDao adUserClickCountDao = DaoFactory.getAdUserClickCountDAO();
                    adUserClickCountDao.updateBatch(adUserClickCounts);
                }
            });
        });

        // 2. 过滤出每个batch中的黑名单用户以生成动态黑名单
        JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(tuple -> {
            String key = tuple._1;
            String[] keySplited = key.split("_");
            String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
            long userid = Long.valueOf(keySplited[1]);
            long adid = Long.valueOf(keySplited[2]);
            // 从mysql中查询指定日期指定用户对指定广告的点击量
            int clickCount = DaoFactory.getAdUserClickCountDao().findClickCountByMultiKey(date, userid, adid);
            // 判断，如果点击量大于等于100则进入黑名单
            if(clickCount >= 100) {
                return (Boolean) true;
            }
            return (Boolean) false;
        });

        // 对blacklistDStream进行去重
        // 20151220_10001_10002 100
        // 20151220_10001_10003 100
        JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(tuple -> {
            String key = tuple._1;
            String[] keySplited = key.split("_");
            Long userid = Long.valueOf(keySplited[1]);
            return userid;
        });
        JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform(rdd -> {
            return rdd.distinct();
        });
        // 将动态黑名单存到mysql
        distinctBlacklistUseridDStream.foreachRDD(rdd -> {
            rdd.foreachPartition(iterator -> {
                List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();
                while(iterator.hasNext()) {
                    long userid = iterator.next();
                    AdBlacklist adBlacklist = new AdBlacklist();
                    adBlacklist.setUserid(userid);
                    adBlacklists.add(adBlacklist);
                }
                DaoFactory.getAdBlacklistDao().insertBatch(adBlacklists);
            });
        });

    }

    /**
     * 根据黑名单对刚刚进来的数据进行过滤
     * @param adRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String, String> filterByBlacklist(
            JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        // 刚刚接受到原始的用户点击行为日志之后
        // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
        // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(
                new Function<JavaPairRDD<String,String>, JavaPairRDD<String,String>>() {
                    private static final long serialVersionUID = 1L;
                    @SuppressWarnings("resource")
                    @Override
                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
                        // 从mysql中查询所有黑名单用户，将其转换为一个rdd
                        List<AdBlacklist> adBlacklists = DaoFactory.getAdBlacklistDao().findAll();
                        List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();
                        for(AdBlacklist adBlacklist : adBlacklists) {
                            tuples.add(new Tuple2<Long, Boolean>(adBlacklist.getUserid(), true));
                        }
                        JavaSparkContext sc = new JavaSparkContext(rdd.context());
                        JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);
                        // 将原始数据rdd映射成<userid, tuple2<string, string>>
                        JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String,String>, Long, Tuple2<String, String>>() {
                            private static final long serialVersionUID = 1L;
                            @Override
                            public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple)
                                    throws Exception {
                                String log = tuple._2;
                                String[] logSplited = log.split(" ");
                                long userid = Long.valueOf(logSplited[3]);
                                return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);
                            }
                        });
                        // 将原始日志数据rdd，与黑名单rdd，进行左外连接
                        // 如果说原始日志的userid，没有在对应的黑名单中，则join不到
                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD =
                                (JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>) mappedRDD.leftOuterJoin(blacklistRDD);
                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(
                                new Function<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, Boolean>() {
                                    private static final long serialVersionUID = 1L;
                                    @Override
                                    public Boolean call(
                                            Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
                                            throws Exception {
                                        Optional<Boolean> optional = tuple._2._2;
                                        // 如果这个值存在，那么说明原始日志中的userid，join到了某个黑名单用户
                                        if(optional.isPresent() && optional.get()) {
                                            return false;
                                        }
                                        return true;
                                    }
                                });
                        JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(
                                new PairFunction<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, String, String>() {
                                    private static final long serialVersionUID = 1L;
                                    @Override
                                    public Tuple2<String, String> call(
                                            Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
                                            throws Exception {
                                        return tuple._2._1;
                                    }
                                });
                        return resultRDD;
                    }
                });
        return filteredAdRealTimeLogDStream;
    }

    // blacklistDStream
    // 里面的每个batch，其实就是都是过滤出来的已经在某天对某个广告点击量超过100的用户
    // 遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
    // 这里一旦增加以后，在整个这段程序的前面，会加上根据黑名单动态过滤用户的逻辑
    // 我们可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里了
    // 所以直接插入mysql即可

    // 我们有没有发现这里有一个小小的问题？
    // blacklistDStream中，可能有userid是重复的，如果直接这样插入的话
    // 那么是不是会发生，插入重复的黑明单用户
    // 我们在插入前要进行去重
    // yyyyMMdd_userid_adid
    // 20151220_10001_10002 100
    // 20151220_10001_10003 100
    // 10001这个userid就重复了

    // 实际上，是要通过对dstream执行操作，对其中的rdd中的userid进行全局的去重



}
