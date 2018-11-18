# UBA-Spark
用户行为分析项目

## 1. 项目介绍

### 1.2 功能
用户在网站内从打开/进入，到做了大量操作，到最后关闭浏览器的访问过程为一次session,该项目就是通过大数据技术，来针对用户的session行为做具体分析展示。
1. 对用户访问session进行分析，筛选出指定的一些用户（有特定年龄、职业、城市）
2. JDBC辅助类封装
3. 用户在指定日期范围内访问session聚合统计，比如，统计出访问时长在0到3s的session占总session数量的比例
4. 按时间比例随机抽取session。
5. 获取点击量、下单量和支付量都排名10的商品种类
6. 获取top10的商品种类的点击数量排名前10的session
7. 复杂性能调优全套解决方案
8. 十亿级数据troubleshooting经验总结
9. 数据倾斜全套完美解决方案
10. 模块功能演示

### 1.1 架构
1. J2EE的平台，通过这个J2EE平台页面可以让使用者，提交各种各样的分析任务，包括用户访问session分析模块；可以指定各种各样的筛选条件，比如年龄范围、职业、城市等等。。
2. J2EE平台接收到了执行统计分析任务的请求之后，会调用底层的封装了spark-submit的shell脚本（Runtime、Process），shell脚本进而提交我们编写的Spark作业。
3. Spark作业获取使用者指定的筛选参数，然后运行复杂的作业逻辑，进行该模块的统计和分析。
4. Spark作业统计和分析的结果，会写入MySQL中指定的表
5. 最后，J2EE平台通过前端页面（美观），以表格、图表的形式展示和查看MySQL中存储的该统计分析任务的结果数据。

## 2. 开发过程

### 2.0 需求及表设计

不能光coding啊，做每个功能前先清楚需求先。

### 2.1 session聚合统计
session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）

#### 2.1.1 按条件筛选session
从数据库获取task参数，然后去筛选session。
要进行session粒度的数据聚合。
首先要从user_visit_action表中，查询出来指定日期范围内的行为数据（过滤）。

1. 第一步：aggregateBySession() 
先将行为数据，按照session_id进行groupByKey分组，此时的数据的粒度就是session粒度了 （通过mapToPair ！！）
然后，可以将session粒度的数据，与用户信息数据，进行join。
就可以获取到session粒度的数据，同时数据里面还包含了session对应的user 和 session搜索行为的信息。

2. 第二步：
```
JavaRDD<Row> actionRDD = getActionRDDByDateRange(taskParam);

JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(actionRDD);
// 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)> 
```

3. 第三步：
针对session粒度的聚合数据，按照平台用户指定的筛选参数进行数据过滤。
通过filter算子，筛选出符合平台用户的筛选参数的 sessionid, AggrInfo 数据。
```
JavaPairRDD<String, String> filteredSessionid2AggrInfo =
                filterSession(sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
```
注意访问外面的任务参数taskParam，要设为final（因为匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的）。


aggregateBySession() 的具体实现里面 三次用到了 mapToPair 算子！！！
用来调整数据结构、分组，形成Tuple2形式的 key,value。
一般的需求基本都要用到这个套路进行数据分组。。。


#### 2.1.2 session聚合统计-- 自定义accumulator
需求：统计6.1过滤出来的session中，访问时长在0s~3s的session的数量，占总session数量的比例。

**原生Accumulator**
如果每种步长的都用一个Accumulator
Accumulator 1s_3s = sc.accumulator(0L);
就要十几个Accumulator。

对过滤以后的session，调用foreach也可以，遍历所有session；
计算每个session的访问时长和访问步长；
访问时长：把session的最后一个action的时间，减去第一个action的时间
访问步长：session的action数量
计算出访问时长和访问步长以后，根据对应的区间，找到对应的Accumulator，然后1s_3s.add(1L)
同时每遍历一个session，就可以给总session数量对应的Accumulator，加1
最后用各个区间的session数量，除以总session数量，就可以计算出各个区间的占比了

这种传统的实现方式，Accumulator太多了，不便于维护。

**自定义Accumulator**
我们自己自定义一个Accumulator，实现较为复杂的复杂分布式计算逻辑，用一个Accumulator维护了所有范围区间的数量的统计逻辑。更方便进行中间状态的维护，而且不用担心并发和锁的问题。
```
Spark2.x的是AccumulatorV2，要实现多个方法。其中add方法
/**
 * 在连接串counter中，找到key对应的value，累加1，然后再更新回counter里面去。
 * 例如传入 1s_3s， 则将 counter中的 1s_3s=0 累加1变成 1s_3s=1
 * @param key 范围key
 */
@Override
public void add(String key) {
    if(StringUtils.isEmpty(counter)){
        return;
    }
    // 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
    String oldValue = StringUtils.getFieldFromConcatString(counter, Constants.REGEX_SPLIT, key);
    if(oldValue != null){
        int newValue = Integer.valueOf(oldValue) + 1;
        String newCounter = StringUtils.setFieldInConcatString(counter, Constants.REGEX_SPLIT, key, String.valueOf(newValue));
        this.counter = newCounter;
    }
}
```

#### 2.1.3 session聚合统计 -- 添加计算时长步长的具体逻辑
在2.1.1基础上添加中添加 
1. 计算session访问步长，访问时长。
2. 在过滤算子计算时使用自定义Accumulator统计访问步长、访问时长。

* 首先注册自定义Accumulator。
```
AccumulatorV2<String, String> sessionAggrStatAccumulator = new UserVisitSessionAccumulator();
sparkSession.sparkContext().register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator");
```
* 然后将Accumulator作为 6.1的 filterSession的参数，修改方法名为filterSessionAndAggrStat，过滤并做聚合信息的统计
```
JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD =
                filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
```

* filterSessionAndAggrStat方法里在filter算子中，在过滤调教最后，取出session中的 visitLength、steplength，进行相应的累加计数。
```
long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
        aggrInfo, Constants.REGEX_SPLIT, Constants.FIELD_VISIT_LENGTH));
long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
        aggrInfo, Constants.REGEX_SPLIT, Constants.FIELD_STEP_LENGTH));
calculateVisitLength(visitLength);
calculateStepLength(stepLength);
sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
```

```
/**
 * 计算访问时长范围
 * @param visitLength
 */
private void calculateVisitLength(long visitLength) {
    if (visitLength > 0 && visitLength < 3) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
    } else if (visitLength >= 4 && visitLength <= 6) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
    } else if (visitLength >= 7 && visitLength <= 9) {
    ...
}
```

Accumulator这种分布式累加计算的变量是懒加载的，需要action算子触发，而运行了多少次action，这个Accumulator就会多运算几次，结果就会出错，所以一般是在插入mysql前执行一次action。
计算出来的结果，在J2EE中，是怎么显示的，是用两张柱状图显示。

要遵循开发Spark大型复杂项目的一些经验准则：
1. 尽量少生成RDD
2. 尽量少对RDD进行算子操作，尽量在一个算子里面，实现多个需要做的功能
3. 尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey（map、mapToPair）。shuffle操作，会导致大量的磁盘读写，严重降低性能。有shuffle的算子，和没有shuffle的算子，性能会有很大的差别。有shfufle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
4. 无论做什么功能，性能第一。

在传统的J2EE或者.NET后者PHP，软件/系统/网站开发中，架构和可维护性，可扩展性的重要程度，远远高于了性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）。
在大数据项目中，比如MapReduce、Hive、Spark、Storm，性能的重要程度远远大于一些代码的规范，和设计模式，代码的划分，类的划分；大数据，大数据，最重要的，就是性能。主要就是因为大数据以及大数据项目的特点，决定了，大数据的程序和项目的速度，都比较慢。如果不优先考虑性能的话，会导致一个大数据处理程序运行时间长度数个小时，甚至数十个小时。此时，对于用户体验，简直就是一场灾难。


### 2.2 session随机抽取
每一次执行用户访问session分析模块，要抽取出100个session。

session随机抽取：按每天的每个小时的session数量，占当天session总数的比例，乘以每天要抽取的session数量，计算出每个小时要抽取的session数量；然后呢，在每天每小时的session中，随机抽取出之前计算出来的数量的session。

举例：10000个session，要取100个session。
0点到1点之间，有2000个session，占总session的比例就是0.2；
按照比例，0点到1点需要抽取出来的session数量是100 * 0.2 = 20个；
然后在0到2000之间产生20个随机数作为索引。
最后，在0点到1点的2000个session中，通过上面的20个索引抽取20个session。

具体步骤：
1. mapToPair得到 time2sessionidRDD 格式： \\<\\yyyy-MM-dd_HH,aggrInfo\\>
2. time2sessionidRDD通过countByKey，可知每个小时的session数量
3. 随机插取算法
4. time2sessionidRDD通过groupByKey，得到time2sessionsRDD，可知每个小时的session Iterable
5. time2sessionsRDD执行flatMapToPair遍历每小时session，在索引上则抽取。
6. 再和action数据join，来得到这些被抽取session的详细行为信息。

不足：
1. 第一个mapToPair返回的应该是Tuple2(dateHour, aggrInfo)比较好，而不是Tuple2(dateHour, sessionId)，可以给以后用。
2. flatMappair不知道什么鬼。
3. join也不会了。


### 2.3 top10热门品类
需求回顾：
通过筛选条件的session访问过的所有品类（点击、下单、支付），按照各个品类的点击、下单和支付次数，降序排序，获取top10热门品类；
点击、下单和支付次数：优先按照点击次数排序、如果点击次数相等，就按照下单次数排序、如果下单次数相当，就按照支付次数排序。

二次排序：
顾名思义，不只是根据一个字段进行一次排序，可能是要根据多个字段，进行多次排序的。点击、下单和支付次数，依次进行排序，就是二次排序。
如果我们就只是根据某一个字段进行排序，比如点击次数降序排序，就不是二次排序。

使用sortByKey算子。
默认情况下，它支持根据int、long等类型来进行排序，但是那样的话，key就只能放一个字段了。
所以需要自定义key，封装n个字段，作为sortByKey算子的key。
并在key中，自己在指定接口方法中，实现自己的根据多字段的排序算法。
然后再使用sortByKey算子进行排序，那么就可以按照我们自己的key，使用多个字段进行排序。

实现思路分析：
1. 拿到通过筛选条件的那批session，访问过的所有品类，和行为action进行join。
2. 得到session访问过的所有品类的点击、下单和支付次数。
3. 自己开发二次排序的key
4. 做映射，将品类的点击、下单和支付次数，封装到二次排序key中，作为PairRDD的key
5. 使用sortByKey(false)，按照自定义key，进行降序二次排序
6. 使用take(10)获取，排序后的前10个品类，就是top10热门品类
7. 将top10热门品类，以及每个品类的点击、下单和支付次数，写入MySQL数据库
8. 本地测试
9. 使用Scala来开发二次排序key

### 2.4 top10活跃session
从上一步的top10热门品类，获取每个Top10Category的点击该品类次数最高的前10个session用户(得到一百个session)，以及其对应的访问明细。

实现思路分析：
1. 拿到符合筛选条件的session的明细数据
2. 按照session粒度进行聚合，groupBySession，再用flatMap算子获取到各session对每个品类的点击次数，返回的是<categoryid,(sessionid,clickCount)>
3. top10品类join一下步骤2，就得到各session对top10品类的点击次数
4. 按照品类id，groupByKey分组取top10，flatMapToPair获取到对这品类点击次数最多的前10个session，直接写入MySQL表；返回的是sessionid
5. 获取各品类top10活跃session的访问明细数据，写入MySQL
6. 本地测试。

做了：
1. 重构一下之前的代码，将通过筛选条件的session的访问明细数据RDD，提取成公共的RDD；这样就不用重复计算同样的RDD
2. 将之前计算出来的top10热门品类的id，生成一个PairRDD，方便后面进行join
3. 车祸现场：最后一步每个品类取top10session的时候，把count作为key来排序了，结果翻车了，每次put入相同count的就覆盖了啊！所以要用sessionId作为key。应该是```count2sessionIdMap.put(sessionId, count);```
