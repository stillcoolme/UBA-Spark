package com.stillcoolme.spark.dao.impl;


import com.stillcoolme.spark.dao.IPageSplitConvertRateDao;
import com.stillcoolme.spark.domain.PageSplitConvertRate;
import com.stillcoolme.spark.helper.JDBCHelper;

/**
 * 页面切片转化率DAO实现类
 *
 * @author Administrator
 */
public class PageSplitConvertRateDaoImpl implements IPageSplitConvertRateDao {

    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {
        String sql = "insert into page_split_convert_rate values(?,?)";
        Object[] params = new Object[]{pageSplitConvertRate.getTaskid(),
                pageSplitConvertRate.getConvertRate()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }

}
