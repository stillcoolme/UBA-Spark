package com.stillcoolme.spark.dao.impl;

import com.stillcoolme.spark.dao.ITop10SessionDao;
import com.stillcoolme.spark.domain.Top10Session;
import com.stillcoolme.spark.helper.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

public class Top10SessionDaoImpl implements ITop10SessionDao {
    @Override
    public void batchInsert(List<Top10Session> top10SessionList) {
        String sql="insert into top10_category_session values(?,?,?,?)";
        List<Object[]> paramList=new ArrayList<Object[]>();
        for(Top10Session top10Session:top10SessionList)
        {
            Object[] param=new Object[]{top10Session.getTaskId(),top10Session.getCategoryId(),
                    top10Session.getSessionId(),top10Session.getClickCount()};
            paramList.add(param);
        }
        JDBCHelper.getInstance().executeBatch(sql,paramList);
    }
    
    public void insert(Top10Session top10Session){
        String sql="insert into top10_category_session values(?,?,?,?)";
        Object[] param=new Object[]{top10Session.getTaskId(),top10Session.getCategoryId(),
                top10Session.getSessionId(),top10Session.getClickCount()};
        JDBCHelper.getInstance().executeUpdate(sql, param);
    }
}
