package com.stillcoolme.spark.dao;

import com.stillcoolme.spark.domain.AdStat;

import java.util.List;

/**
 * 广告实时统计DAO接口
 * @author Administrator
 *
 */
public interface IAdStatDao {

	void updateBatch(List<AdStat> adStats);
	
}
