package com.stillcoolme.spark.dao;


import com.stillcoolme.spark.domain.SessionRandomExtract;

import java.util.List;

public interface ISessionRandomExtractDao {
    void batchInsert(List<SessionRandomExtract> sessionRandomExtractList);

    void insert(SessionRandomExtract sessionRandomExtract);
}
