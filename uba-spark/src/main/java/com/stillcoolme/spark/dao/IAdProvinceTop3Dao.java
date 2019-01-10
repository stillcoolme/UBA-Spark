package com.stillcoolme.spark.dao;

import com.stillcoolme.spark.domain.AdProvinceTop3;

import java.util.List;

/**
 * 各省份top3热门广告DAO接口
 * @author Administrator
 *
 */
public interface IAdProvinceTop3Dao {

	void updateBatch(List<AdProvinceTop3> adProvinceTop3s);
	
}
