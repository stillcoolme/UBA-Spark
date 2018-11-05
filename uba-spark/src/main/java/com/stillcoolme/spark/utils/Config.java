package com.stillcoolme.spark.utils;


/**
 * @author xiekunyuan
 * @date: 2018/2/7 15:30
 **/
public class Config {

    public  static  MyProperties jdbcProps;
    public static MyProperties sparkProps;

    static{
        jdbcProps= MyProperties.getInstance("jdbc.properties");
        sparkProps = MyProperties.getInstance("sparkService.properties");
    }
}
