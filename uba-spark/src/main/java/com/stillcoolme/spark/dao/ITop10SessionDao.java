package com.stillcoolme.spark.dao;

import com.stillcoolme.spark.domain.Top10Session;

import java.util.List;

public interface ITop10SessionDao {
    void batchInsert(List<Top10Session> top10SessionList);

    void insert(Top10Session top10Session);

}
