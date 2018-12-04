package com.stillcoolme.spark.dao;

import com.stillcoolme.spark.domain.AreaTop3Product;

import java.util.List;

/**
 * 各区域top3热门商品DAO接口
 * @author Administrator
 *
 */
public interface IAreaTop3ProductDao {

	void insertBatch(List<AreaTop3Product> areaTopsProducts);
	
}
