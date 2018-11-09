package com.stillcoolme.spark.factory;

import com.stillcoolme.spark.dao.ISessionAggrStatDAO;
import com.stillcoolme.spark.dao.ISessionDetailDAO;
import com.stillcoolme.spark.dao.ISessionRandomExtractDAO;
import com.stillcoolme.spark.dao.ITaskDAO;
import com.stillcoolme.spark.dao.SessionDetailDAOImpl;
import com.stillcoolme.spark.dao.impl.SessionAggrStatDAOImpl;
import com.stillcoolme.spark.dao.impl.SessionRandomExtractDAOImpl;
import com.stillcoolme.spark.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {

	/**
	 * 获取任务管理DAO
	 * @return DAO
	 */
	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO(){
		return new SessionAggrStatDAOImpl();
	}

	public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
		return new SessionRandomExtractDAOImpl();
	}

	public static ISessionDetailDAO getSessionDetailDAO(){
		return new SessionDetailDAOImpl();
	}
}
