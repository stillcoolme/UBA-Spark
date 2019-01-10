package com.stillcoolme.spark.utils;


/**
 * @author stillcoolme
 * @date: 2018/2/7 15:30
 **/
public class Config {

    public static MyProperties jdbcProps;
    public static MyProperties sparkProps;
    public static MyProperties kafkaProps;

    static{
        jdbcProps= MyProperties.getInstance("jdbc.properties");
        sparkProps = MyProperties.getInstance("sparkService.properties");
        kafkaProps = MyProperties.getInstance("kafka.properties");
    }
}
