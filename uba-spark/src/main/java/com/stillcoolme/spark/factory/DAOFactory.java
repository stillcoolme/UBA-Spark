package com.stillcoolme.spark.factory;

import com.stillcoolme.spark.dao.ITaskDAO;
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
	
}
