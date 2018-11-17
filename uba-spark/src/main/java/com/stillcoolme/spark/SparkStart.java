package com.stillcoolme.spark;

import com.stillcoolme.spark.constant.Constants;
import com.stillcoolme.spark.entity.ReqEntity;
import com.stillcoolme.spark.service.BaseService;
import com.stillcoolme.spark.service.session.UserVisitSessionAnalyze;
import com.stillcoolme.spark.utils.Config;
import com.stillcoolme.spark.utils.ConfigurationManager;
import com.stillcoolme.spark.utils.DateUtils;
import com.stillcoolme.spark.utils.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.Date;

import static com.stillcoolme.spark.service.BaseService.javaSparkContext;
import static com.stillcoolme.spark.service.BaseService.sqlContext;

/**
 * Created by zhangjianhua on 2018/10/30.
 */
public class SparkStart {

    public static void main(String[] args) {

        String appName = Config.sparkProps.getProperty("spark.appName");
        appName = String.format("%s-%s", appName, DateUtils.formatDateTime(new Date()));
        String runMode = Config.sparkProps.getProperty(Constants.SPARK_MASTER);

        SparkConf conf = new SparkConf().setMaster(runMode).setAppName(appName);

        BaseService.sparkSession = SparkSession.builder().config(conf).getOrCreate();
        BaseService.javaSparkContext = new JavaSparkContext(BaseService.sparkSession.sparkContext());
        BaseService.sqlContext = BaseService.sparkSession.sqlContext();
        BaseService.reader = sqlContext.read().format("jdbc");
        BaseService.reader.option("url", Config.jdbcProps.getProperty("jdbc.url"));          //数据库路径
        BaseService.reader.option("driver", Config.jdbcProps.getProperty("jdbc.driver"));
        BaseService.reader.option("user", Config.jdbcProps.getProperty("jdbc.user"));
        BaseService.reader.option("password", Config.jdbcProps.getProperty("jdbc.password"));

        // 生成模拟测试数据
        mockData(javaSparkContext, sqlContext);

        BaseService baseService = new UserVisitSessionAnalyze();
        ReqEntity reqEntity = new ReqEntity();
        reqEntity.setReqData("[{\"taskId\":2}]");
        baseService.run(reqEntity);

        // 关闭Spark上下文
        javaSparkContext.close();
    }

    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     * @param sc SparkContext
     * @return SQLContext
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        String runMode = Config.sparkProps.getProperty(Constants.SPARK_MASTER);
        if(runMode.equals("local")) {
            return BaseService.sparkSession.sqlContext();
        } else {
//            return new HiveContext(sc);
            return null;
        }
    }

    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        String runMode = Config.sparkProps.getProperty(Constants.SPARK_MASTER);
        if(runMode.equals("local")) {
            MockData.mock(sc, sqlContext);
        }
    }


}
