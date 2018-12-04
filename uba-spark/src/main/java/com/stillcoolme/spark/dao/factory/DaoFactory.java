package com.stillcoolme.spark.dao.factory;

import com.stillcoolme.spark.dao.IAreaTop3ProductDao;
import com.stillcoolme.spark.dao.IPageSplitConvertRateDao;
import com.stillcoolme.spark.dao.ISessionAggrStatDao;
import com.stillcoolme.spark.dao.ISessionDetailDao;
import com.stillcoolme.spark.dao.ISessionRandomExtractDao;
import com.stillcoolme.spark.dao.ITaskDao;
import com.stillcoolme.spark.dao.ITop10CategoryDao;
import com.stillcoolme.spark.dao.ITop10SessionDao;
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
}
