package com.stillcoolme.spark.dao;

import com.stillcoolme.spark.domain.AdClickTrend;

import java.util.List;

/**
 * 广告点击趋势DAO接口
 * @author Administrator
 *
 */
public interface IAdClickTrendDao {

	void updateBatch(List<AdClickTrend> adClickTrends);
	
}
