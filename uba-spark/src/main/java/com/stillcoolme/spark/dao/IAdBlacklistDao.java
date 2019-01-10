package com.stillcoolme.spark.dao;

import com.stillcoolme.spark.domain.AdBlacklist;

import java.util.List;

/**
 * @author stillcoolme
 * @date 2018/12/25 22:37
 */
public interface IAdBlacklistDao {

    void insertBatch(List<AdBlacklist> adBlacklists);

    List<AdBlacklist> findAll();
}
