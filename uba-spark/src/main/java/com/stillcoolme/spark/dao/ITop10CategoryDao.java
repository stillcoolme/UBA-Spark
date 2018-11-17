package com.stillcoolme.spark.dao;

import com.stillcoolme.spark.domain.Top10Category;

import java.util.List;

public interface ITop10CategoryDao {
    void insert(Top10Category top10Category);
    void batchInsert(List<Top10Category> top10CategoryList);
}
