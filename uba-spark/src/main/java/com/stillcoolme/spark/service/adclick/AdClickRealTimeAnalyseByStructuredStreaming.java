package com.stillcoolme.spark.service.adclick;

import com.stillcoolme.spark.constant.Constants;
import com.stillcoolme.spark.entity.ReqEntity;
import com.stillcoolme.spark.entity.RespEntity;
import com.stillcoolme.spark.service.BaseService;
import com.stillcoolme.spark.utils.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaPairDStream;


public class AdClickRealTimeAnalyseByStructuredStreaming extends BaseService {

    @Override
    public RespEntity run(ReqEntity req) {

        // 创建针对Kafka数据来源的输入DStream（离线流，代表了一个源源不断的数据来源，抽象）
        // 选用kafka direct api（很多好处，包括自己内部自适应调整每次接收数据量的特性，等等）
        // 构建kafka参数map
        // 主要要放置的就是，你要连接的kafka集群的地址（broker集群的地址列表）
        //kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST, Config.kafkaProps.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));

        Dataset<Row> ds = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("subscribe", Config.kafkaProps.getProperty(Constants.KAFKA_TOPICS))
                .load();
        Dataset<String> adRealTimeLogDs = ds.selectExpr("CAST(value AS STRING)").as(Encoders.STRING());


        // 根据动态黑名单进行数据过滤
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream =
                filterByBlacklist(adRealTimeLogDs);

        return null;

    }

    /**
     * 根据黑名单对刚刚进来的数据进行过滤
     * @param adRealTimeLogDs
     * @return
     */
    private JavaPairDStream<String, String> filterByBlacklist(Dataset<String> adRealTimeLogDs) {
        return null;


    }

}
