package com.stillcoolme.spark.utils;

import com.stillcoolme.spark.constant.Constants;

/**
 * 校验工具类
 * @author Administrator
 *
 */
public class ValidUtils {
	
	/**
	 * 校验数据中的指定字段，是否在指定范围内
	 * @param data 数据
	 * @param dataField 数据字段
	 * @param parameter 参数
	 * @param startParamField 起始参数字段
	 * @param endParamField 结束参数字段
	 * @return 校验结果
	 */
	public static boolean between(String data, String dataField, String parameter, String startParamField, String endParamField){
        // 先取出这两个，要是空就不用再继续了 ！！！！
        String startParamFieldStr = StringUtils.getFieldFromConcatString(
                parameter, Constants.REGEX_SPLIT, startParamField);
        String endParamFieldStr = StringUtils.getFieldFromConcatString(
                parameter, Constants.REGEX_SPLIT, endParamField);
        if(startParamFieldStr == null || endParamFieldStr == null) {
            return true;
        }
        int startParamFieldValue = Integer.valueOf(startParamFieldStr);
        int endParamFieldValue = Integer.valueOf(endParamFieldStr);

        String dataFieldStr = StringUtils.getFieldFromConcatString(data, Constants.REGEX_SPLIT, dataField);
        if(dataFieldStr != null){
            int dataFieldValue = Integer.valueOf(dataFieldStr);
            if(dataFieldValue >= startParamFieldValue &&
                    dataFieldValue <= endParamFieldValue) {
                return true;
            } else {
                return false;
            }
        }
        return false;
	}

    /**
     * 校验数据中的指定字段，是否有值与参数字段的值相同
     * data是用户操作的日志数据， param是前台查询任务传过来的参数
     * @param data 数据
     * @param dataField 数据字段
     * @param parameter 参数
     * @param paramField 参数字段
     * @return 校验结果
     */
    public static boolean in(String data, String dataField, String parameter, String paramField){
        String paramValue = StringUtils.getFieldFromConcatString(parameter, Constants.REGEX_SPLIT, paramField);
        // !!! 这居然返回true
        if(paramValue == null){
            return true;
        }
        String[] paramFieldValueSplited = paramValue.split(Constants.REGEX_DATAAPPEND);
        String dataFieldValue = StringUtils.getFieldFromConcatString(
                data, "\\|", dataField);
        if(dataFieldValue != null) {
            String[] dataFieldValueSplited = dataFieldValue.split(Constants.REGEX_DATAAPPEND);
            for(String singleDataFieldValue : dataFieldValueSplited) {
                for(String singleParamFieldValue : paramFieldValueSplited) {
                    if(singleDataFieldValue.equals(singleParamFieldValue)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * 校验数据中的指定字段，是否在指定范围内
     * @param data 数据
     * @param dataField 数据字段
     * @param parameter 参数
     * @param paramField 参数字段
     * @return 校验结果
     */
    public static boolean equal(String data, String dataField, String parameter, String paramField){
        String paramFieldValue = StringUtils.getFieldFromConcatString(
                parameter, "\\|", paramField);
        if(paramFieldValue == null) {
            return true;
        }
        String dataFieldValue = StringUtils.getFieldFromConcatString(
                data, "\\|", dataField);
        if(dataFieldValue != null) {
            if(dataFieldValue.equals(paramFieldValue)) {
                return true;
            }
        }
        return false;
    }

}
