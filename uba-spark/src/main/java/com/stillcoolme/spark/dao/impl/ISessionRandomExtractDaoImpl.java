package com.stillcoolme.spark.dao.impl;


import com.stillcoolme.spark.dao.ISessionRandomExtractDao;
import com.stillcoolme.spark.domain.SessionRandomExtract;
import com.stillcoolme.spark.helper.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class ISessionRandomExtractDaoImpl implements ISessionRandomExtractDao {
    @Override
    public void batchInsert(List<SessionRandomExtract> sessionRandomExtractList) {
        String sql="insert into session_random_extract values(?,?,?,?,?)";
        List<Object[]> paramList=new ArrayList<Object[]>();
        for (SessionRandomExtract sessionRandomExtract:sessionRandomExtractList) {
            Object[] params=new Object[]{sessionRandomExtract.getTaskId(),sessionRandomExtract.getSessionId()
            ,sessionRandomExtract.getStartTime(),sessionRandomExtract.getSearchKeyWords(),sessionRandomExtract.getClick_category_ids()};
            paramList.add(params);
        }
        JDBCHelper.getInstance().executeBatch(sql,paramList);
    }

    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql="insert into session_random_extract values(?,?,?,?,?)";
        Object[] params=new Object[]{sessionRandomExtract.getTaskId(),sessionRandomExtract.getSessionId()
                ,sessionRandomExtract.getStartTime(),sessionRandomExtract.getSearchKeyWords(),sessionRandomExtract.getClick_category_ids()};
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
