package com.stillcoolme.spark.dao;

import java.util.List;

import com.stillcoolme.spark.domain.AdUserClickCount;

/**
 * 用户广告点击量DAO接口
 * @author Administrator
 *
 */
public interface IAdUserClickCountDao {

	void updateBatch(List<AdUserClickCount> adUserClickCounts);

    int findClickCountByMultiKey(String date, long userid, long adid);
}
