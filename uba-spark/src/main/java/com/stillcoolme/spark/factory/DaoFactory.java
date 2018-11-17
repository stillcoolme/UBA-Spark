package com.stillcoolme.spark.factory;

import com.stillcoolme.spark.dao.ISessionAggrStatDao;
import com.stillcoolme.spark.dao.ISessionDetailDao;
import com.stillcoolme.spark.dao.ISessionRandomExtractDao;
import com.stillcoolme.spark.dao.ITaskDao;
import com.stillcoolme.spark.dao.impl.SessionDetailDaoImpl;
import com.stillcoolme.spark.dao.ITop10CategoryDao;
import com.stillcoolme.spark.dao.impl.SessionAggrStatDaoImpl;
import com.stillcoolme.spark.dao.impl.SessionRandomExtractDaoImpl;
import com.stillcoolme.spark.dao.impl.TaskDaoImpl;
import com.stillcoolme.spark.dao.impl.Top10CategoryDaoImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DaoFactory {

	/**
	 * 获取任务管理DAO
	 * @return DAO
	 */
	public static ITaskDao getTaskDao() {
		return new TaskDaoImpl();
	}

	public static ISessionAggrStatDao getSessionAggrStatDao(){
		return new SessionAggrStatDaoImpl();
	}

	public static ISessionRandomExtractDao getSessionRandomExtractDao(){
		return new SessionRandomExtractDaoImpl();
	}

	public static ISessionDetailDao getSessionDetailDao(){
		return new SessionDetailDaoImpl();
	}

	public static ITop10CategoryDao getTop10CategoryDao(){ return new Top10CategoryDaoImpl();}

}
