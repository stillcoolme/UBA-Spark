package com.stillcoolme.spark.dao;

import com.stillcoolme.spark.domain.SessionAggrStat;

/**
 * Created by zhangjianhua on 2018/11/7.
 */
public interface ISessionAggrStatDao {

    /**
     * 插入session聚合统计结果
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat);

}
