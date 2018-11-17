package com.stillcoolme.spark.service;

import com.stillcoolme.spark.dao.ITaskDao;
import com.stillcoolme.spark.entity.ReqEntity;
import com.stillcoolme.spark.entity.RespEntity;
import com.stillcoolme.spark.factory.DaoFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Created by zhangjianhua on 2018/10/30.
 */
public abstract class BaseService implements java.io.Serializable {

    public static SparkSession sparkSession = null;
    public static JavaSparkContext javaSparkContext = null;
    public static SQLContext sqlContext = null;

    public static DataFrameReader reader = null;

    // 创建需要使用的DAO组件
    public ITaskDao taskDAO = DaoFactory.getTaskDao();

    public abstract RespEntity run(ReqEntity req);


}
