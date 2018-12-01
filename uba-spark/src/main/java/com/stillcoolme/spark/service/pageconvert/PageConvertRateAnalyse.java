package com.stillcoolme.spark.service.pageconvert;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stillcoolme.spark.constant.Constants;
import com.stillcoolme.spark.dao.IPageSplitConvertRateDao;
import com.stillcoolme.spark.dao.factory.DaoFactory;
import com.stillcoolme.spark.domain.PageSplitConvertRate;
import com.stillcoolme.spark.domain.Task;
import com.stillcoolme.spark.entity.ReqEntity;
import com.stillcoolme.spark.entity.RespEntity;
import com.stillcoolme.spark.service.BaseService;
import com.stillcoolme.spark.service.session.UserVisitSessionAnalyze;
import com.stillcoolme.spark.utils.DateUtils;
import com.stillcoolme.spark.utils.NumberUtils;
import com.stillcoolme.spark.utils.ParamUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PageConvertRateAnalyse extends BaseService {
    private final static Logger LOG = Logger.getLogger(PageConvertRateAnalyse.class);
    @Override
    public RespEntity run(ReqEntity req) {
        RespEntity resEntity = new RespEntity();
        JSONArray jsonArray = JSONArray.parseArray(req.reqData);
        JSONObject lstCondition = jsonArray.getJSONObject(0);
        Long taskid = lstCondition.getLong("taskId");
        if (taskid == null) {
            return null;
        }
        Task task = taskDao.findById(taskid);
        if(task == null) {
            LOG.warn(new Date() + ": cannot find this task with id [" + taskid + "].");
            return null;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 进行session粒度的数据聚合,首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = UserVisitSessionAnalyze.getActionRDDByDateRange(taskParam);

        // 获取用户访问行为，计算出各个session在指定页面流中的页面切片的访问量；实现页面单跳切片生成以及页面流匹配的算法。
        // 1. 获得<sessionid,访问行为> RDD
        JavaPairRDD<String, Row> sessionid2actionRDD = UserVisitSessionAnalyze.getSessionid2ActionRDD(actionRDD);
        // 2. 做一次groupByKey操作，因为我们要拿到每个session对应的访问行为数据，才能够去生成切片
        JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2actionRDD.groupByKey();
        // 3. 每个session的单跳页面切片的生成，以及页面流的匹配算法
        JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(sessionid2actionsRDD, taskParam);
        LOG.warn("pageSplitRDD" + pageSplitRDD.count());
        //  这里就是全部session的pageSplitPvMap了吧？？
        Map<String, Long> pageSplitPvMap = pageSplitRDD.countByKey();
        // 4. 从 每个session对应的访问行为数据 获取页面流中初始页面的pv
        long startPagePv = getStartPagePv(taskParam, sessionid2actionsRDD);
        // 5. 计算目标页面流的各个页面切片的转化率
        Map<String, Double> convertRateMap = getPageConvertRatio(taskParam, startPagePv, pageSplitPvMap);
        // 6. 持久化页面切片转化率
        persistConvertRate(taskid, convertRateMap);
        LOG.warn("持久化页面切片转化率完成");
        List resultList = new ArrayList();
        resultList.add(convertRateMap);
        resEntity.setResData(new JSONArray(resultList).toString());
        resEntity.setSuccess(true);
        return resEntity;
    }

    private void persistConvertRate(Long taskid, Map<String, Double> convertRateMap) {
        StringBuffer buffer = new StringBuffer("");
        for(Map.Entry<String, Double> convertRateEntry: convertRateMap.entrySet()){
            String pageSplit = convertRateEntry.getKey();
            double convertRate = convertRateEntry.getValue();
            buffer.append(pageSplit + "=" + convertRate + "|");
        }
        String convertRate = buffer.toString();
        convertRate = convertRate.substring(0, convertRate.length() - 1);
        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskid(taskid);
        pageSplitConvertRate.setConvertRate(convertRate);

        IPageSplitConvertRateDao pageSplitConvertRateDAO = DaoFactory.getPageSplitConvertRateDao();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);



    }

    private Map<String, Double> getPageConvertRatio(JSONObject taskParam, long startPagePv, Map<String, Long> pageSplitPvMap) {
        // 存放结果！
        Map<String, Double> convertRateMap = new HashMap<String, Double>();
        String[] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");
        long lastPageSplitPv = 0;
        // 用户调教的目标page 3,5,2,4,6
        // 3_5的跳转率：
        // 3_5 pv / 3 pv
        for (int i = 1; i < targetPages.length; i++) {
            String targetPageSplit = targetPages[i-1] + "_" + targetPages[i];
            long targetPageSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));
            double convertRate = 0.0;
            if(i == 1) {
                convertRate = NumberUtils.formatDouble(
                        (double)targetPageSplitPv / (double)startPagePv, 2);
            } else {
                convertRate = NumberUtils.formatDouble(
                        (double)targetPageSplitPv / (double)lastPageSplitPv, 2);
            }
            convertRateMap.put(targetPageSplit, convertRate);
            lastPageSplitPv = targetPageSplitPv;
        }
        return convertRateMap;
    }

    /**
     * 获取页面流中初始页面的pv
     * @param taskParam
     * @param sessionid2actionsRDD
     * @return
     */
    private long getStartPagePv(JSONObject taskParam, JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD) {
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);
        JavaRDD<Long> startPageRDD =  sessionid2actionsRDD.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                List<Long> list = new ArrayList<Long>();
                Iterator<Row> iterator = tuple._2.iterator();
                while(iterator.hasNext()) {
                    Row row = iterator.next();
                    long pageid = row.getLong(3);
                    if(pageid == startPageId) {
                        list.add(pageid);
                    }
                }
                return list.iterator();
            }
        });
        return startPageRDD.count();
    }

    /**
     * 页面切片生成与匹配算法
     * @param sessionid2actionsRDD
     * @param taskParam
     * @return
     */
    private JavaPairRDD<String, Integer> generateAndMatchPageSplit(JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD, JSONObject taskParam) {
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Broadcast<String> targetPageFlowBroadcast = BaseService.javaSparkContext.broadcast(targetPageFlow);

        return sessionid2actionsRDD.flatMapToPair(
            new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                    // 定义返回list
                    List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
                    String sessionid = tuple._1;
                    // 获取到当前session的访问行为的迭代器
                    Iterator<Row> iterator = tuple._2.iterator();
                    // 获取使用者指定的页面流,3,4,5
                    String[] targetPages = targetPageFlowBroadcast.value().split(",");
                    // 对session的访问行为数据按照时间进行排序? 怎么排比较优雅？
                    List<Row> rows = new ArrayList<Row>();
                    while(iterator.hasNext()) {
                        rows.add(iterator.next());
                    }
                    Collections.sort(rows, new Comparator<Row>() {
                        @Override
                        public int compare(Row o1, Row o2) {
                            String actionTime1 = o1.getString(4);
                            String actionTime2 = o2.getString(4);

                            Date date1 = DateUtils.parseTime(actionTime1);
                            Date date2 = DateUtils.parseTime(actionTime2);

                            return (int)(date1.getTime() - date2.getTime());
                        }
                    });
                    // 页面切片的生成，以及页面流的匹配
                    Long lastPageId = null;
                    for(Row row : rows) {
                        long pageid = row.getLong(3);
                        if(lastPageId == null){
                            lastPageId = pageid;
                            continue;
                        }
                        // 对用户访问的页面 3,5,2,1,8,9 每次循环生成一个页面切片，lastPageId=3，然后5，切片，3_5
                        String pageSplit = lastPageId + "_" + pageid;
                        // 对这个切片判断一下，是否在用户指定的页面流中
                        for(int i = 1; i < targetPages.length; i++) {
                            // 比如说，用户指定的页面流是3,2,5,8,1，从索引1开始遍历
                            String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
                            if(pageSplit.equals(targetPageSplit)) {
                                list.add(new Tuple2<String, Integer>(pageSplit, 1));
                                break;
                            }
                        }
                        lastPageId = pageid;
                    }
                    return list.iterator();
                }
            }
        );
    }
}
