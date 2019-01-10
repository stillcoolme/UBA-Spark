package com.stillcoolme.spark.dao.factory;

import com.stillcoolme.spark.dao.IAdBlacklistDao;
import com.stillcoolme.spark.dao.IAdClickTrendDao;
import com.stillcoolme.spark.dao.IAdProvinceTop3Dao;
import com.stillcoolme.spark.dao.IAdStatDao;
import com.stillcoolme.spark.dao.IAdUserClickCountDao;
import com.stillcoolme.spark.dao.IAreaTop3ProductDao;
import com.stillcoolme.spark.dao.IPageSplitConvertRateDao;
import com.stillcoolme.spark.dao.ISessionAggrStatDao;
import com.stillcoolme.spark.dao.ISessionDetailDao;
import com.stillcoolme.spark.dao.ISessionRandomExtractDao;
import com.stillcoolme.spark.dao.ITaskDao;
import com.stillcoolme.spark.dao.ITop10CategoryDao;
import com.stillcoolme.spark.dao.ITop10SessionDao;
import com.stillcoolme.spark.dao.impl.AdBlacklistDaoImpl;
import com.stillcoolme.spark.dao.impl.AdClickTrendDaoImpl;
import com.stillcoolme.spark.dao.impl.AdProvinceTop3DaoImpl;
import com.stillcoolme.spark.dao.impl.AdStatDaoImpl;
import com.stillcoolme.spark.dao.impl.AdUserClickCountDaoImpl;
import com.stillcoolme.spark.dao.impl.AreaTop3ProductDaoImpl;
import com.stillcoolme.spark.dao.impl.PageSplitConvertRateDaoImpl;
import com.stillcoolme.spark.dao.impl.SessionAggrStatDaoImpl;
import com.stillcoolme.spark.dao.impl.SessionDetailDaoImpl;
import com.stillcoolme.spark.dao.impl.ISessionRandomExtractDaoImpl;
import com.stillcoolme.spark.dao.impl.TaskDaoImpl;
import com.stillcoolme.spark.dao.impl.Top10CategoryDaoImpl;
import com.stillcoolme.spark.dao.impl.Top10SessionDaoImpl;

public class DaoFactory {
    /**
     * 使用工厂模式
     * @return
     */
    public static ITaskDao getTaskDao()
    {
        return new TaskDaoImpl();
    }

    public static ISessionAggrStatDao getSessionAggrStatDao()
    {
        return new SessionAggrStatDaoImpl();
    }

    public static ISessionRandomExtractDao getSessionRandomExtractDao(){
        return new ISessionRandomExtractDaoImpl();
    }

    public  static ISessionDetailDao getSessionDetailDao()
    {
        return new SessionDetailDaoImpl();
    }

    public static ITop10CategoryDao getTop10CategoryDao(){ return new Top10CategoryDaoImpl();}

    public static ITop10SessionDao getTop10SessionDao(){ return new Top10SessionDaoImpl();}

    public static IPageSplitConvertRateDao getPageSplitConvertRateDao(){
        return new PageSplitConvertRateDaoImpl();
    }

    public static IAreaTop3ProductDao getAreaTop3ProductDao(){
        return new AreaTop3ProductDaoImpl();
    }

    public static IAdUserClickCountDao getAdUserClickCountDAO() {
        return new AdUserClickCountDaoImpl();
    }

    public static IAdUserClickCountDao getAdUserClickCountDao() {
        return new AdUserClickCountDaoImpl();
    }

    public static IAdBlacklistDao getAdBlacklistDao() {
        return new AdBlacklistDaoImpl();
    }

    public static IAdStatDao getAdStatDao() {
        return new AdStatDaoImpl();
    }

    public static IAdProvinceTop3Dao getAdProvinceTop3Dao() {
        return new AdProvinceTop3DaoImpl();
    }

    public static IAdClickTrendDao getAdClickTrendDAO() {
        return new AdClickTrendDaoImpl();
    }
}
