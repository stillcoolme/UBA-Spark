package com.stillcoolme.spark.service.session;

import com.stillcoolme.spark.constant.Constants;
import com.stillcoolme.spark.utils.StringUtils;
import org.apache.spark.util.AccumulatorV2;
import parquet.org.slf4j.Logger;
import parquet.org.slf4j.LoggerFactory;

/**
 *  Accumulator还可以用来更新mysql
 * Created by zhangjianhua on 2018/11/5.
 */
public class UserVisitSessionAccumulator extends AccumulatorV2<String, String>{

    private static Logger logger = LoggerFactory.getLogger(UserVisitSessionAccumulator.class);

    String counter =  Constants.SESSION_COUNT + "=0|"
            + Constants.TIME_PERIOD_1s_3s + "=0|"
            + Constants.TIME_PERIOD_4s_6s + "=0|"
            + Constants.TIME_PERIOD_7s_9s + "=0|"
            + Constants.TIME_PERIOD_10s_30s + "=0|"
            + Constants.TIME_PERIOD_30s_60s + "=0|"
            + Constants.TIME_PERIOD_1m_3m + "=0|"
            + Constants.TIME_PERIOD_3m_10m + "=0|"
            + Constants.TIME_PERIOD_10m_30m + "=0|"
            + Constants.TIME_PERIOD_30m + "=0|"
            + Constants.STEP_PERIOD_1_3 + "=0|"
            + Constants.STEP_PERIOD_4_6 + "=0|"
            + Constants.STEP_PERIOD_7_9 + "=0|"
            + Constants.STEP_PERIOD_10_30 + "=0|"
            + Constants.STEP_PERIOD_30_60 + "=0|"
            + Constants.STEP_PERIOD_60 + "=0";

    @Override
    public boolean isZero() {
        return counter ==  Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    //拷贝这个累加器
    @Override
    public AccumulatorV2<String, String> copy() {
        UserVisitSessionAccumulator newAccumulator  = new UserVisitSessionAccumulator();
        newAccumulator .counter = this.counter;
        return newAccumulator;
    }

    /**
     * 要在counter中，找到key对应的value，累加1，然后再更新回连接串里面去。
     * 传入 1s_3s， 则将 counter中的 1s_3s=0 累加1变成 1s_3s=1
     * @param key 范围key
     */
    @Override
    public void add(String key) {
        if(StringUtils.isEmpty(counter)){
            return;
        }
        // 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
        String oldValue = StringUtils.getFieldFromConcatString(counter, Constants.REGEX_SPLIT, key);
        if(oldValue != null){
            int newValue = Integer.valueOf(oldValue) + 1;
            String newCounter = StringUtils.setFieldInConcatString(counter, Constants.REGEX_SPLIT, key, String.valueOf(newValue));
            this.counter = newCounter;
        }

    }



    @Override
    public void reset() {
        counter =  Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    // 这里边other代表的是另外的相同类型的累加器，
    // 这里进行的是每个累加器的合并，相当于对上面三个str进行合并
    @Override
    public void merge(AccumulatorV2<String, String> o) {
        UserVisitSessionAccumulator other = (UserVisitSessionAccumulator) o;
        String otherCounter = other.counter;
        String[] otherCounters = otherCounter.split(Constants.REGEX_SPLIT);
        String[] counters = counter.split(Constants.REGEX_SPLIT);
        for(String c1:counters){
            String[] c1s = c1.split(Constants.REGEX_EQUAL);
            for(String c2: otherCounters){
                String[] c2s = c2.split(Constants.REGEX_EQUAL);
                if(c1s[0].equals(c2s[0])){
                    int newValue = Integer.valueOf(c1s[1]) + Integer.valueOf(c2s[1]);
                    counter = StringUtils.setFieldInConcatString(
                            counter, Constants.REGEX_SPLIT, c1s[0], String.valueOf(newValue));
                }
            }
        }
    }

    @Override
    public String value() {
        return counter;
    }


}
