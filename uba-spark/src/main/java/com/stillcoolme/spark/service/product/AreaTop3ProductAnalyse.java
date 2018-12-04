package com.stillcoolme.spark.service.product;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stillcoolme.spark.constant.Constants;
import com.stillcoolme.spark.dao.IAreaTop3ProductDao;
import com.stillcoolme.spark.dao.factory.DaoFactory;
import com.stillcoolme.spark.domain.AreaTop3Product;
import com.stillcoolme.spark.domain.Task;
import com.stillcoolme.spark.entity.ReqEntity;
import com.stillcoolme.spark.entity.RespEntity;
import com.stillcoolme.spark.service.BaseService;
import com.stillcoolme.spark.utils.ParamUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AreaTop3ProductAnalyse extends BaseService {
    private final static Logger LOG = Logger.getLogger(AreaTop3ProductAnalyse.class);

    @Override
    public RespEntity run(ReqEntity req) {
        RespEntity resEntity = new RespEntity();
        JSONArray jsonArray = JSONArray.parseArray(req.reqData);
        JSONObject lstCondition = jsonArray.getJSONObject(0);
        Long taskid = lstCondition.getLong("taskId");
        if (taskid == null) {
            return null;
        }
        Task task = taskDao.findById(taskid);
        if (task == null) {
            LOG.warn(new Date() + ": cannot find this task with id [" + taskid + "].");
            return null;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        // 1. 获取点击行为数据
        JavaPairRDD<Long, Row> cityid2clickActionRDD = getcityid2ClickActionRDDByDate(taskParam);
        // 2. 异构数据源之sparksql从MySQL中查询城市数据
        JavaPairRDD<Long, Row> cityid2cityInfoRDD = getCityid2CityInfoRDD();
        LOG.info("city: " + cityid2cityInfoRDD.count());
        // 3. 关联城市信息以及RDD转换为DataFrame后，注册临时表 tmp_click_product_basic
        joinCityAndActionInfo(cityid2clickActionRDD, cityid2cityInfoRDD);
        // 4. 生成各区域各商品点击次数 和 拼接城市列表 的临时表
        generateTempAreaPrdocutClickCountTable();
        // 5. 关联商品信息并使用自定义get_json_object函数和内置if函数标记经营类型
        linkProductInfo();
        // 6. 使用开窗函数统计各区域的top3热门商品，以前都练习过的又忘记了。。。
        // 使用内置case when函数给各个区域打上级别标记， 对area打标记，得到area_leve
        JavaRDD areaTop3ProductRDD = getAreaTop3Product();
        // 这边的写入mysql和之前不太一样，因为这个业务需求而言，计算出来的最终数据量是比较小的
        // 总共不到10个区域的top3热门商品，最后数据量是几十个，所以可以直接将数据collect()到本地，用批量插入的方式，一次性插入mysql即可。
        List<Row> rows = areaTop3ProductRDD.collect();
        persistAreaTop3Product(taskid, rows);
        return null;
    }

    private void persistAreaTop3Product(Long taskid, List<Row> rows) {
        List<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();
        for(Row row : rows) {
            AreaTop3Product areaTop3Product = new AreaTop3Product();
            areaTop3Product.setTaskid(taskid);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductid(row.getLong(2));
            areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));
            areaTop3Product.setCityInfos(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));
            areaTop3Products.add(areaTop3Product);
        }
        IAreaTop3ProductDao areTop3ProductDAO = DaoFactory.getAreaTop3ProductDao();
        areTop3ProductDAO.insertBatch(areaTop3Products);
    }

    /**
     * 获取各区域top3热门商品
     * @return
     */
    private JavaRDD<Row> getAreaTop3Product() {
        // 技术点：开窗函数, case when
        // 使用开窗函数先进行一个子查询
        // 按照area进行分组，给每个分组内的数据，按照点击次数降序排序，打上一个组内的行号
        // 接着在外层查询中，过滤出各个组内的行号排名前3的数据，得到各个区域下top3热门商品
        // 再使用 case when 各个区域打上级别标记。根据多个条件，不同的条件对应不同的值
        String sql =
                "SELECT area, "
                        + "CASE "
                        + "WHEN area = 'China South' OR area = 'China East' THEN 'A级地区'"
                        + "WHEN area = 'China North' OR area = 'China Middle' THEN 'B级地区'"
                        + "WHEN area = 'WEST NORTH' THEN 'C级地区'"
                        + "WHEN area = 'WEST SOUTH' THEN 'D级地区'"
                        + "ELSE 'E级地区' END as area_level, "
                        + "product_id, "
                        + "click_count, "
                        + "city_infos, "
                        + "product_name, "
                        + "product_status "
                + "FROM "
                + "(SELECT area, "
                        + "product_id,"
                        + "click_count,"
                        + "city_infos,"
                        + "product_name,"
                        + "product_status,"
                        +  "Row_Number() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
                        +  "FROM " + Constants.TABLE_AREA_FULLPROD_CLICK_COUNT
                + ") t "
                + "WHERE rank <= 3";
        Dataset<Row> df = sparkSession.sql(sql);
        LOG.warn("各区域Top3商品 统计表 AreaTop3Product: ");
        df.show();
        return df.javaRDD();
    }

    private void linkProductInfo() {
        // product_id去关联商品信息表，product_id，product_name和product_status
        // product_status要特殊处理，0，1，分别代表了自营和第三方的商品，放在了一个json串里面
        // get_json_object()函数，可以从json串中获取指定的字段的值
        // if()函数，判断，如果product_status是0，那么就是自营商品；如果是1，那么就是第三方商品
        // 最后表结构 area, product_id, product_name, click_count, city_infos, product_status
        Dataset<Row> productInfoDF = reader.option("dbtable", "product_info").format("jdbc").load();
        productInfoDF.createOrReplaceTempView(Constants.TABLE_PRODUCT_INFO);
        // 与 各区域商品点击表 关联
        String sql = "SELECT " +
                " apcc.area, apcc.product_id, apcc.click_count, apcc.city_infos, pi.product_name, " +
                " if(get_json_object(pi.info, 'product_status')=0, '自营商品', '第三方商品') product_status " +
                " FROM " + Constants.TABLE_AREA_PRODUCT_CLICK_COUNT + " apcc JOIN " + Constants.TABLE_PRODUCT_INFO +
                " pi ON apcc.product_id = pi.id";
        Dataset<Row> df = sparkSession.sql(sql);
        df.createOrReplaceTempView(Constants.TABLE_AREA_FULLPROD_CLICK_COUNT);
        LOG.warn("各区域内各商品点击数 关联了 商品信息表后的 统计表：" + Constants.TABLE_AREA_FULLPROD_CLICK_COUNT);
        df.show();
    }

    /**
     * 生成各区域各商品点击次数临时表
     */
    private void generateTempAreaPrdocutClickCountTable() {
        // 按照area和product_id两个字段进行分组
        // 计算出各区域各商品的点击次数
        // 可以获取到每个area下的每个product_id的城市信息拼接起来的串
        String sql = "SELECT area, product_id, count(*) click_count, " +
                "group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos " +
                "FROM " + Constants.TABLE_CLICK_PRODUCT_BASIC +
                " GROUP BY area,product_id ";
        // 使用Spark SQL执行这条SQL语句
        Dataset<Row> df = sparkSession.sql(sql);
        // 再次将查询出来的数据注册为一个临时表
        // 各区域各商品的点击次数（以及额外的城市列表）
        df.createOrReplaceTempView(Constants.TABLE_AREA_PRODUCT_CLICK_COUNT);
        LOG.warn("各区域内各商品点击数 统计表：" + Constants.TABLE_AREA_PRODUCT_CLICK_COUNT);
        df.show();
    }

    /**
     * 生成点击商品基础信息临时表
     * @param cityid2clickActionRDD
     * @param cityid2cityInfoRDD
     */
    private void joinCityAndActionInfo(JavaPairRDD<Long, Row> cityid2clickActionRDD, JavaPairRDD<Long, Row> cityid2cityInfoRDD) {
        // 执行join操作，进行点击行为数据和城市数据的关联
        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = (JavaPairRDD<Long, Tuple2<Row, Row>>) cityid2clickActionRDD.join(cityid2cityInfoRDD);
        // 将上面的JavaPairRDD，转换成一个JavaRDD<Row>，然后才能转换为DataFrame ！！
        JavaRDD<Row> mappedRDD = joinedRDD.map(new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
                long cityid = tuple._1;
                Row clickAction = tuple._2._1;
                Row cityInfo = tuple._2._2;
                long productid = clickAction.getLong(1);
                String cityName = cityInfo.getString(1);
                String area = cityInfo.getString(2);
                return RowFactory.create(cityid, cityName, area, productid);
            }
        });
        // 基于JavaRDD<Row>的格式，就可以将其转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));
        StructType schema = DataTypes.createStructType(structFields);
        Dataset<Row> df = sparkSession.createDataFrame(mappedRDD, schema);
        // 将DataFrame中的数据，注册成临时表（tmp_clk_prod_basic）
        df.createOrReplaceTempView(Constants.TABLE_CLICK_PRODUCT_BASIC);
        LOG.warn("点击的商品id 基础表: " + Constants.TABLE_CLICK_PRODUCT_BASIC);
        df.show();
    }


    /**
     * 使用Spark SQL从MySQL中查询城市信息
     */
    private JavaPairRDD<Long, Row> getCityid2CityInfoRDD() {
        // 通过SQLContext去从MySQL中查询数据
        Dataset<Row> cityInfoDF = reader.option("dbtable", "city_info").format("jdbc").load();
        // 返回RDD
        JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();
        JavaPairRDD<Long, Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(
            new PairFunction<Row, Long, Row>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Tuple2<Long, Row> call(Row row) throws Exception {
                    long cityid = row.getLong(0);
                    return new Tuple2<Long, Row>(cityid, row);
                }
         });
        return cityid2cityInfoRDD;
    }

    /**
     * 查询指定日期范围内的点击行为数据
     * @return 点击行为数据
     */
    private JavaPairRDD<Long, Row> getcityid2ClickActionRDDByDate(JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        // 从user_visit_action中，查询用户访问行为数据
        // 第一个限定：click_product_id，限定为不为空的访问行为，那么就代表着点击行为
        // 第二个限定：在用户指定的日期范围内的数据
        String sql =
                "SELECT "
                        + "city_id, "
                        + "click_product_id product_id "
                        + "FROM user_visit_action "
                        + "WHERE click_product_id IS NOT NULL "
//                        + "AND click_product_id != 'NULL' "
//                        + "AND click_product_id != 'null' "
                        + "AND action_time >='" + startDate + "' "
                        + "AND action_time <='" + endDate + "'";
        Dataset<Row> clickActionDF = sqlContext.sql(sql);
        // repartition 解决 Spark SQL可能出现低并行度的性能问题
        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD().repartition(20);
        JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        Long cityid = row.getLong(0);
                        return new Tuple2<Long, Row>(cityid, row);
                    }
                });
        return cityid2clickActionRDD;
    }
}
