package com.stillcoolme.spark.constant;

/**
 * 常量接口
 *
 * @author Administrator
 */
public interface Constants {

    /**
     * 配置文件
     */
    String JDBC_PROPS = "jdbc.properties";

    /**
     * 字符串分隔相关的符号常量
     */
    String REGEX_SPLIT = "\\|";
    String REGEX_EQUAL = "=";
    //相同key的多个值用，来相连
    String REGEX_DATAAPPEND = ",";

    /**
     * 项目配置相关的常量
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";
    String JDBC_URL_PROD = "jdbc.url.prod";
    String JDBC_USER_PROD = "jdbc.user.prod";
    String JDBC_PASSWORD_PROD = "jdbc.password.prod";
    String SPARK_MASTER = "spark.master";
    String SPARK_LOCAL_TASKID_SESSION = "spark.local.taskid.session";
    String SPARK_LOCAL_TASKID_PAGE = "spark.local.taskid.page";
    String SPARK_LOCAL_TASKID_PRODUCT = "spark.local.taskid.product";
    String KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list";
    String KAFKA_TOPICS = "kafka.topics";

    /**
     * Spark作业相关的常量
     */
    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";
    String SPARK_APP_NAME_PAGE = "PageOneStepConvertRateSpark";
    String FIELD_SESSION_ID = "sessionid";
    String FIELD_SEARCH_KEYWORDS = "searchKeywords";
    String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
    String FIELD_AGE = "age";
    String FIELD_PROFESSIONAL = "professional";
    String FIELD_CITY = "city";
    String FIELD_SEX = "sex";
    String FIELD_VISIT_LENGTH = "visitLength";
    String FIELD_STEP_LENGTH = "stepLength";
    String FIELD_START_TIME = "startTime";
    String FIELD_CLICK_COUNT = "clickCount";
    String FIELD_ORDER_COUNT = "orderCount";
    String FIELD_PAY_COUNT = "payCount";
    String FIELD_CATEGORY_ID = "categoryid";

    String SESSION_COUNT = "session_count";
    // 时长常量
    String TIME_PERIOD_1s_3s = "1s_3s";
    String TIME_PERIOD_4s_6s = "4s_6s";
    String TIME_PERIOD_7s_9s = "7s_9s";
    String TIME_PERIOD_10s_30s = "10s_30s";
    String TIME_PERIOD_30s_60s = "30s_60s";
    String TIME_PERIOD_1m_3m = "1m_3m";
    String TIME_PERIOD_3m_10m = "3m_10m";
    String TIME_PERIOD_10m_30m = "10m_30m";
    String TIME_PERIOD_30m = "30m";
    //步长常量
    String STEP_PERIOD_1_3 = "1_3";
    String STEP_PERIOD_4_6 = "4_6";
    String STEP_PERIOD_7_9 = "7_9";
    String STEP_PERIOD_10_30 = "10_30";
    String STEP_PERIOD_30_60 = "30_60";
    String STEP_PERIOD_60 = "60";

    /**
     * 任务相关的常量
     */
    String PARAM_START_DATE = "startDate";
    String PARAM_END_DATE = "endDate";
    String PARAM_START_AGE = "startAge";
    String PARAM_END_AGE = "endAge";
    String PARAM_PROFESSIONALS = "professionals";
    String PARAM_CITIES = "cities";
    String PARAM_SEX = "sex";
    String PARAM_KEYWORDS = "keywords";
    String PARAM_CATEGORY_IDS = "categoryIds";
    String PARAM_TARGET_PAGE_FLOW = "targetPageFlow";

    /**
     * 热门产品中使用的表格
     */
    // 点击的商品id 基础表
    String TABLE_CLICK_PRODUCT_BASIC = "tmp_click_product_basic";
    // 各区域内各商品点击数 统计表
    String TABLE_AREA_PRODUCT_CLICK_COUNT = "tmp_area_product_click_count";
    // 商品信息表
    String TABLE_PRODUCT_INFO = "tmp_product_info";
    // 各区域内各商品点击数 关联了 商品信息表后的 表
    String TABLE_AREA_FULLPROD_CLICK_COUNT = "tmp_area_fullprod_click_count";
}
