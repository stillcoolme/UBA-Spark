package com.stillcoolme.spark.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author stillcoolme
 * @date 2019/1/27 10:57
 */
public class CalanderUtils {

    public static String YRAR_CYCLE = "year";
    public static String MONTH_CYCLE = "month";
    public static String WEEK_CYCLE = "week";
    public static String DAY_CYCLE = "day";

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     *
     * @param cycle  周期类型
     * @param cycleNum 周期数
     * @return 返回每个周期的开始时间
     */
    public static String getLastCycle(String cycle, int cycleNum) {
        Calendar calendar = Calendar.getInstance();
        if("year".equals(cycle)){
            // 获取本年1月1号0点
            calendar.set(calendar.get(Calendar.YEAR), 0, 1, 0, 0, 0);
            // 根据 before,来返回几个周期以前
            calendar.add(Calendar.YEAR, cycleNum);
        }else if ("month".equals(cycle)) {
            // 获取本月1号0点
            calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), 1, 0, 0, 0);
            // 根据 before,来返回几个周期以前
            calendar.add(Calendar.MONTH, cycleNum);

        } else if("week".equals(cycle)){
            // 获取现在是 本月的第几天
            calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
            // 是以 周日为一周的开始的？？
            calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
            // 获取cycleNum周的零点
            calendar.add(Calendar.WEEK_OF_MONTH, cycleNum);

        } else if("day".equals(cycle)){
            // 获取今天零点
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            // 获取cycleNum天的零点
            calendar.add(Calendar.DAY_OF_YEAR, cycleNum);
        } else {
            return "no such cycle";
        }
        Date date = calendar.getTime();
        return sdf.format(date);
    }


    public static void main(String[] args) {
        System.out.println(getLastCycle("year", -1));
        System.out.println(getLastCycle("month", -1));
        System.out.println(getLastCycle("week", -1));
        System.out.println(getLastCycle("day", -1));
    }

}
