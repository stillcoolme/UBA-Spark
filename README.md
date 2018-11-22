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


## 3. 性能调优
接下来要做什么？
按照本人开发过的大量的单个spark作业，处理10亿到100亿级别数据的经验，要针对我们写好的spark作业程序，实施十几个到二十个左右的复杂性调优技术；
1. 性能调优相关的原理讲解；
2. 性能调优技术的实施；
3. 实际经验中应用性能调优技术的经验总结；
4. 掌握一整套复杂的Spark企业级性能调优解决方案；而不只是简单的一些性能调优技巧。
5. 数据倾斜解决方案：针对写好的spark作业，实施一整套数据倾斜解决方案：实际经验中积累的数据倾斜现象的表现，以及处理后的效果总结
6. troubleshooting：针对写好的spark作业，讲解实际经验中遇到的各种线上报错问题，以及解决方案
7. 生产环境测试：Hive表

调优，你要对spark的作业流程清楚：
* Driver到executor的结构；
```
Master: Driver
    |-- Worker: Executor
            |-- job
                 |-- stage
                       |-- Task Task 
```
* 作业划分为task分到executor上（一个cpu core执行一个task）；
* 一个Stage内，最终的RDD有多少个partition，就会产生多少个task
* BlockManager负责Executor，task的数据管理，task来它这里拿数据；


### 3.0 集群启动
先部署spark集群，确保mysql启动。

对项目打包，打出spark-uba.jar包。
另外依赖jar包打到lib目录，需要以下两个
fastjson-1.2.31.jar
mysql-connector-java-5.1.46.jar

然后编写start.sh启动脚本提交到集群即可。。
```
BASEPATH=$(cd `dirname $0`; pwd)
SPARK_BIN=/data/spark-2.1.1-bin-hadoop2.6

executor_memory=4g
master_ip=manager

mkdir -p ${BASEPATH}/logs/

${SPARK_BIN}/bin/spark-submit \
--jars $(echo ${BASEPATH}/lib/*.jar | tr ' ' ',')  \
--class com.stillcoolme.spark.SparkStart  \
--total-executor-cores 6 \
--executor-cores 2 \
--executor-memory $executor_memory \
--master spark://$master_ip:7077  ${BASEPATH}/uba-spark-1.0.0.jar \
>> ${BASEPATH}/logs/spark-uba.log 2>&1
```



### 3.1 资源分配
性能调优的王道：分配更多资源。
只有资源分配好了，才能给更好的给以后各个调优点打好基础。spark作业能够分配的资源达到了最大后，那么才是考虑去做后面的这些性能调优的点。

1、分配哪些资源？ executor、cpu per executor、memory per executor、driver memory
2、在哪里分配这些资源？
在我们在生产环境中，提交spark作业时，用的spark-submit shell脚本，里面调整对应的参数

/usr/local/spark/bin/spark-submit \
--class cn.spark.sparktest.core.WordCountCluster \
--driver-memory 100m \  配置driver的内存（影响不大）
--num-executors 3 \  配置executor的数量
--executor-memory 100m \  配置每个executor的内存大小
--executor-cores 3 \  配置每个executor的cpu core数量
/usr/local/SparkTest-0.0.1-SNAPSHOT-jar-with-dependencies.jar \

3、调节到多大，算是最大呢？
第一种，Spark Standalone，公司集群上，搭建了一套Spark集群，你心里应该清楚每台机器还能够给你使用的，大概有多少内存，多少cpu core；
那么，设置的时候，就根据这个实际的情况，去调节每个spark作业的资源分配。比如说你的每台机器能够给你使用4G内存，2个cpu core；20台机器；executor，20；4G内存，2个cpu core，平均每个executor。

第二种，Yarn。资源队列。资源调度。应该去查看spark作业，要提交到的资源队列，大概有多少资源？500G内存，100个cpu core；executor，50；10G内存，2个cpu core，平均每个executor。

一个原则，你能使用的资源有多大，就尽量去调节到最大的大小（executor的数量，几十个到上百个不等；executor内存；executor cpu core最大）

4、为什么调节了资源以后，性能可以提升？
* executor-cores（增加每个executor的cpu core，增加了执行的并行能力）。
原本20个executor，各有2个cpu core。能够并行40个task。
现在每个executor的cpu core，增加到了5个。就能够并行执行100个task。
执行的速度，提升了2.5倍。
**但不超过服务器的cpu core数，不然会waiting**。

* executor-memory（增加每个executor的内存量）。
增加了内存量以后，对性能的提升，有两点：
1、如果需要对RDD进行cache，那么更多的内存，就可以缓存更多的数据，将更少的数据写入磁盘，甚至不写入磁盘。
2、对于shuffle操作，reduce端，需要内存来存放拉取的数据并进行聚合。如果内存不够，会写入磁盘。如果给executor分配更多内存，减少了磁盘IO，提升了性能。
3、对于task的执行，会创建很多对象。如果内存比较小，可能会频繁导致JVM堆内存满了，然后频繁GC，垃圾回收，minor GC和full GC。（速度很慢）。内存加大以后，带来更少的GC，垃圾回收，避免了速度变慢，速度变快了。
**但是不超过分配各每个worker的内存**

* num-executors （增加executor个数)
如果executor数量比较少，那么，能够并行执行的task数量就比较少，就意味着，我们的Application的并行执行的能力就很弱。
比如有3个executor，每个executor有2个cpu core，那么同时能够并行执行的task，就是6个。6个执行完以后，再换下一批6个task。

问题：
1. 对spark的架构居然都不太熟悉，一个Master，多个Worker；一个Driver，多个Executor。

### 3.2 调节并行度

并行度：Spark作业中会拆成多个stage，各个stage的task数量，也就代表了job在各个阶段（stage）的并行度。（job划分成stage是根据宽窄依赖来区分的）
**spark自己的算法给task选择Executor，Execuotr进程里面包含着task线程**。
举例（wordCount）：
    map阶段：每个task根据去找到自己需要的数据写到文件去处理。生成的文件一定是存放相同的key对应的values，相同key的values一定是进入同一个文件。
    reduce阶段：每个stage1的task，会去上面生成的文件拉取数据；拉取到的数据，一定是相同key对应的数据。对相同的key，对应的values，才能去执行我们自定义的function操作（_ + _）


假设资源调到上限了，如果不调节并行度，导致并行度过低，会怎么样？

假设task设置了100个task。有50个executor，每个executor有3个cpu core。
则Application任何一个stage运行的时候，都有总数在150个cpu core，可以并行运行。
但是共有100个task，平均分配一下，每个executor分配到2个task。
那么同时在运行的task，只有100个，每个executor只会并行运行2个task。每个executor剩下的一个cpu core，并行度没有与资源相匹配，就浪费掉了。

合理的并行度的设置，应该要设置到可以完全合理的利用你的集群资源；
比如上面的例子，总共集群有150个cpu core，可以并行运行150个task。
即可以同时并行运行，还可以让每个task要处理的数据量变少。
最终，就是提升你的整个Spark作业的性能和运行速度。

**官方是推荐，task数量，设置成spark application总cpu core数量的2~3倍，比如150个cpu core，基本要设置task数量为300~500；**

因为有些task会运行的快一点，比如50s就完了，有些task，可能会慢一点，要1分半才运行完，如果task数量设置成cpu core总数的2~3倍，那么一个task运行完了以后，另一个task马上可以补上来，就尽量不让cpu core空闲，尽量提升spark作业运行的效率和速度，提升性能。

3、如何设置一个Spark Application的并行度？
SparkConf conf = new SparkConf()
  .set("spark.default.parallelism", "500")

“重剑无锋”：真正有分量的一些技术和点，其实都是看起来比较平凡，看起来没有那么“炫酷”，但是其实是你每次写完一个spark作业，进入性能调优阶段的时候，应该优先调节的事情，就是这些。


### 3.3重构RDD架构以及RDD持久化

第一，RDD架构重构与优化
尽量去复用RDD，差不多的RDD抽取为一个共同的RDD，供后面的RDD计算时，反复使用。
第二，公共RDD一定要实现持久化
持久化，也就是说，将RDD的数据缓存到内存中/磁盘中，（BlockManager），以后无论对这个RDD做多少次计算，那么都是直接取这个RDD的持久化的数据，比如从内存中或者磁盘中，直接提取一份数据。

第三，持久化是可以进行序列化的
如果正常将数据持久化在内存中，可能会导致内存的占用过大，导致OOM。

当纯内存无法支撑公共RDD数据完全存放的时候，就优先考虑，使用序列化的方式在纯内存中存储。将RDD的每个partition的数据，序列化成一个大的字节数组，就一个对象；序列化后，大大减少内存的空间占用。
序列化的方式，唯一的缺点就是，在获取数据的时候，需要反序列化。

如果序列化纯内存方式，还是导致OOM，内存溢出；就只能考虑磁盘的方式，内存+磁盘的普通方式（无序列化）。
内存+磁盘，序列化

第四，为了数据的高可靠性，而且内存充足，可以使用双副本机制，进行持久化
持久化的双副本机制，持久化后的一个副本，因为机器宕机了，副本丢了，就还是得重新计算一次；持久化的每个数据单元，存储一份副本，放在其他节点上面；从而进行容错；一个副本丢了，不用重新计算，还可以使用另外一份副本。这种方式，仅仅针对你的内存资源极度充足。


### 3.4 大变量进行广播， 使用Kryo序列化， 本地化等待时间

session分析模块中随机抽取部分，time2sessionsRDD.flatMapToPair()，取session2extractlistMap中对应时间的list。
task执行的算子flatMapToPair算子，**使用了外部的变量session2extractlistMap**，每个task都要通过网络的传输获取一份变量的副本。占网络资源占内存。
所以引入：
广播变量，在driver上会有一份初始的副本。然后给每个节点的executor一份副本。就可以让变量产生的副本大大减少。

广播变量初始的时候，就在Drvier上有一份副本。
task在运行的时候，想要使用广播变量中的数据，此时首先会在自己本地的Executor对应的BlockManager中（负责管理某个Executor对应的内存和磁盘上的数据），尝试获取变量副本；
如果本地没有，那么就从Driver远程拉取变量副本，并保存在本地的BlockManager中；
此后这个executor上的task，都会直接使用本地的BlockManager中的副本。
BlockManager除了从driver上拉取，也可能从其他节点的BlockManager上拉取变量副本，举例越近越好。

==============

进一步优化，优化序列化格式。
默认情况下，Spark内部是使用Java的对象输入输出流序列化机制，ObjectOutputStream / ObjectInputStream
这种默认序列化机制的好处在于，处理起来比较方便；也不需要我们手动去做什么事情，只是，你在算子里面使用的变量，必须是实现Serializable接口的，可序列化即可。但是默认的序列化机制的效率不高速度慢；序列化数据占用的内存空间大。

Spark支持使用Kryo序列化机制。
Kryo序列化机制，比默认的Java序列化机制，速度要快，序列化后的数据要更小，大概是Java序列化机制的1/10。让网络传输的数据变少；耗费的内存资源大大减少。

Kryo序列化机制，一旦启用以后，会生效的几个地方：
1、算子函数中使用到的外部变量
2、持久化RDD时进行序列化，StorageLevel.MEMORY_ONLY_SER
3、shuffle

第一步：```SparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")```
第二步，注册你使用到的，需要通过Kryo序列化的自定义类。
```SparkConf.registerKryoClasses(new Class[]{CategorySortKey.class});
```
Kryo要求，如果要达到它的最佳性能的话，那么就一定要注册你自定义的类（比如，你的算子函数中使用到了外部自定义类型的对象变量，这时，就要求必须注册你的类，否则Kryo达不到最佳性能）。

============

fastutil优化

fastutil是扩展了Java标准集合框架（Map、List、Set；HashMap、ArrayList、HashSet）的类库，提供了特殊类型的map、set、list和queue；能够提供更小的内存占用，更快的存取速度。
fastutil的每一种集合类型，都实现了对应的Java中的标准接口（比如fastutil的map，实现了Java的Map接口），因此可以直接放入已有系统的任何代码中。
fastutil还提供了一些JDK标准类库中没有的额外功能（比如双向迭代器）。
fastutil除了对象和原始类型为元素的集合，fastutil也提供引用类型的支持，但是对引用类型是使用等于号（=）进行比较的，而不是equals()方法。

Spark中应用fastutil的场景：
1、如果算子函数使用了外部变量是某种比较大的集合，那么可以考虑使用fastutil改写外部变量，首先从源头上就减少内存的占用，通过广播变量进一步减少内存占用，再通过Kryo序列化类库进一步减少内存占用。
2、在你的算子函数里，也就是task要执行的计算逻辑里面，要创建比较大的Map、List等集合，可以考虑将这些集合类型使用fastutil类库重写，减少task创建出来的集合类型的内存占用。

fastutil的使用，在pom.xml中引用fastutil的包
```List<Integer> => IntList
```
基本都是类似于IntList的格式，前缀就是集合的元素类型；特殊的就是Map，Int2IntMap，代表了key-value映射的元素类型。

==============

调节数据本地化等待时长

背景：Driver对Application的每一个stage的task，进行分配之前，都会计算出每个task要计算的是哪个分片数据，RDD的某个partition。分配算法会希望每个task正好分配到它要计算的数据所在的节点。
但是可能某task要分配过去的那个节点的计算资源和计算能力都满了。**这种时候，Spark会等待一段时间**，默认情况下是3s，**到最后实在是等待不了了，就会选择一个比较差的本地化级别**，将task分配到靠它要计算的数据所在节点，比较近的一个节点，然后进行计算。
task会通过其所在节点的BlockManager来获取数据，BlockManager发现自己本地没有数据，会通过一个getRemote()方法，通过TransferService（网络数据传输组件）从数据所在节点的BlockManager中，获取数据，通过网络传输回task所在节点。

我们调节等待时长就是让spark再等待多一下，不要到低一级的本地化级别。

**怎么调节spark.locality.wait**
spark.locality.wait，默认是3s；
```
spark.locality.wait.process
spark.locality.wait.node
spark.locality.wait.rack

new SparkConf()
  .set("spark.locality.wait", "10")
```

先用client模式，在本地就直接可以看到比较全的日志。
观察日志，spark作业的运行日志显示，观察大部分task的数据本地化级别。starting task。PROCESS LOCAL、NODE LOCAL。
如果大多都是PROCESS_LOCAL，那就不用调节了。
如果是发现，好多的级别都是NODE_LOCAL、ANY，那么最好就去调节一下数据本地化的等待时长。
看看大部分的task的本地化级别有没有提升；看看，整个spark作业的运行时间有没有缩短。如果spark作业的运行时间反而增加了，那就还是不要调节了。

**本地化级别**
* PROCESS_LOCAL：进程本地化，代码和数据在同一个进程中，也就是在同一个executor中；计算数据的task由executor执行，数据在executor的BlockManager中；性能最好
* NODE_LOCAL：节点本地化，代码和数据在同一个节点中；比如说，数据作为一个HDFS block块，就在节点上，而task在节点上某个executor中运行；或者是，数据和task在一个节点上的不同executor中；数据需要在进程间进行传输
* NO_PREF：对于task来说，数据从哪里获取都一样，没有好坏之分
* RACK_LOCAL：机架本地化，数据和task在一个机架的两个节点上；数据需要通过网络在节点之间进行传输
* ANY：数据和task可能在集群中的任何地方，而且不在一个机架中，性能最差



### 3.5 JVM调优

**降低cache操作的内存占比**

每一次放对象的时候，都是放入eden区域，和其中一个survivor1 区域；另外一个survivor2 区域是空闲的。

当eden区域和一个survivor区域放满了以后，就会触发minor gc。
如果你的JVM内存不够大的话，可能导致频繁的年轻代OOM，频繁的进行minor gc。
频繁的minor gc会导致短时间内，有些存活的对象，多次垃圾回收都没有回收掉。会导致这种短声明周期对象，年龄过大，垃圾回收次数太多还没有回收到。

minor gc后会将存活下来的对象，放入之前空闲的那一个survivor区域中。
这里可能会出现一个问题。
默认eden、survior1和survivor2的内存占比是8:1:1。问题是，如果存活下来的对象是1.5，一个survivor区域放不下。
此时就可能通过JVM的担保机制，将多余的对象，直接放入老年代了。导致老年代囤积一大堆，短生命周期的，本来应该在年轻代中的，可能马上就要被回收掉的对象。

老年代有许多对象可能导致老年代频繁满溢。频繁进行full gc。
full gc由于这个算法的设计，考虑老年代中的对象数量很少，满溢进行full gc的频率应该很少，因此采取了不太复杂，但是耗费性能和时间的垃圾回收算法。

内存不充足的时候，问题：
1、频繁minor gc，也会导致频繁spark停止工作
2、老年代囤积大量活跃对象（短生命周期的对象），导致频繁full gc，导致jvm的工作线程停止工作，spark停止工作。等着垃圾回收结束。


spark中，堆内存又被划分成了两块，一块儿是专门用来给RDD的cache、persist操作进行RDD数据缓存用的；另外一块，用来给spark算子函数的运行使用的，存放函数中自己创建的对象。
默认情况下，给RDD cache操作的内存占比是60%。但是如果某些情况下，task算子函数中创建的对象过多，然后内存又不太大，导致了频繁的minor gc，甚至频繁full gc，导致spark频繁的停止工作。性能影响会很大。

针对上述这种情况，大家可以在之前我们讲过的那个spark ui。yarn去运行的话，那么就通过yarn的界面，去查看你的spark作业的运行统计，很简单，大家一层一层点击进去就好。可以看到每个stage的运行情况，包括每个task的运行时间、gc时间等等。如果发现gc太频繁，时间太长。此时就可以适当调价这个比例。

降低cache操作的内存占比，大不了用persist操作，选择将一部分缓存的RDD数据写入磁盘，或者序列化方式，配合Kryo序列化类，减少RDD缓存的内存占用；降低cache操作内存占比；对应的，算子函数的内存占比就提升了。
一句话，让task执行算子函数时，有更多的内存可以使用。
spark-submit脚本里面，去用--conf的方式，去添加配置：
```
--conf spark.storage.memoryFraction，0.6 -> 0.5 -> 0.4 -> 0.2
--conf spark.shuffle.memoryFraction=0.3
```


====================

**调节executor堆外内存**

executor堆外内存

有时候，如果你的spark作业处理的数据量特别特别大；然后spark作业一运行，时不时的报错，shuffle file cannot find，executor、task lost，out of memory（内存溢出）；

可能是说executor的堆外内存不太够用，导致executor在运行的过程中会内存溢出；
然后导致后续的stage的task在运行的时候，可能要从一些executor中去拉取shuffle map output文件，但是executor可能已经挂掉了，关联的Block manager也没有了；spark作业彻底崩溃。

上述情况下，就可以去考虑调节一下executor的堆外内存。
避免掉某些JVM OOM的异常问题。
此外，堆外内存调节的比较大的时候，对于性能来说，也会带来一定的提升。


spark-submit脚本里面，去用--conf的方式，去添加基于yarn的提交模式配置；
```
--conf spark.yarn.executor.memoryOverhead=2048
```
默认情况下，这个堆外内存上限大概是300多M；真正处理大数据的时候，这里都会出现问题，导致spark作业反复崩溃，无法运行；此时就会去调节这个参数到至少1G（1024M），甚至说2G、4G。


========================

**调节连接等待时长**

我们知道 Executor，优先从自己本地关联的BlockManager中获取某份数据。
如果本地block manager没有的话，那么会通过TransferService，去远程连接其他节点上executor的block manager去获取。
正好碰到那个exeuctor的JVM在垃圾回收，就会没有响应，无法建立网络连接；
spark默认的网络连接的超时时长，是60s；
就会出现某某file。一串file id。uuid（dsfsfd-2342vs--sdf--sdfsd）。not found。file lost。
报错几次，几次都拉取不到数据的话，可能会导致spark作业的崩溃。也可能会导致DAGScheduler，反复提交几次stage。TaskScheduler，反复提交几次task。大大延长我们的spark作业的运行时间。

可以考虑在spark-submit脚本里面，调节连接的超时时长：
```
--conf spark.core.connection.ack.wait.timeout=300
```



### 3.6 Suffle调优

#### 3.6.1 shuffle流程
shuffle，一定是分为两个stage来完成的。因为这其实是个逆向的过程，不是stage决定shuffle，是shuffle决定stage。
在某个action触发job的时候，DAGScheduler会负责划分job为多个stage。划分的依据，就是如果发现有会触发shuffle操作的算子（reduceByKey）。
就将这个操作的前半部分，以及之前所有的RDD和transformation操作，划分为一个stage；
shuffle操作的后半部分，以及后面的，直到action为止的RDD和transformation操作，划分为另外一个stage。

每一个shuffle的前半部分stage的task，**每个task**都会**创建下一个stage的task数量相同的文件**，比如下一个stage会有10个task，那么当前stage每个task都会创建10份文件；会将同一个key对应的values，写入同一个文件中的；
不同节点上的task，也一定会将同一个key对应的values，写入下一个stage的同一个task对应的文件中。

shuffle的后半部分stage的task，每个task都会从各个节点上的task写的属于自己的那一份文件中，拉取key, value对；
然后task会有一个内存缓冲区，然后会用HashMap，进行key, values的汇聚；
task会用我们自己定义的聚合函数reduceByKey(_+_)，把所有values进行一对一的累加；聚合出来最终的值。


#### 3.6.2 合并map端输出文件

```sparConf().set("spark.shuffle.consolidateFiles", "true")
```

开启了map端输出文件的合并机制之后：

第一个stage，同时运行cpu core个task，比如cpu core是2个，并行运行2个task；每个task都创建下一个stage的task数量个文件；

第一个stage，并行运行的2个task执行完以后；就会执行另外两个task；**另外2个task不会再重新创建输出文件；而是复用之前的task创建的map端输出文件，将数据写入上一批task的输出文件中**。

第二个stage，task在拉取数据的时候，就不会去拉取上一个stage每一个task为自己创建的那份输出文件了；而是拉取少量的输出文件，每个输出文件中，可能包含了多个task给自己的map端输出。



#### 3.6.3 调节map端内存缓冲与reduce端内存占比
深入一下shuffle原理：
shuffle的map task：
输出到磁盘文件的时候，统一都会先写入每个task自己关联的一个内存缓冲区。
这个缓冲区大小，默认是32kb。当**内存缓冲区满溢之后，会进行spill溢写操作到磁盘文件中去**。数据量比较大的情况下可能导致多次溢写。

shuffle的reduce端task：
在拉取到数据之后，会用hashmap的数据格式，来对各个key对应的values进行汇聚的时候。
使用的就是自己对应的executor的内存，executor（jvm进程，堆），默认executor内存中划分给reduce task进行聚合的比例，是0.2。要是拉取过来的数据很多，那么在内存中，放不下；就将在内存放不下的数据，都spill（溢写）到磁盘文件中去。数据大的时候磁盘上溢写的数据量越大，后面在进行聚合操作的时候，很可能会多次读取磁盘中的数据，进行聚合。

**怎么调节？**
看Spark UI，shuffle的磁盘读写的数据量很大，就意味着最好调节一些shuffle的参数。进行调优。
```
set(spark.shuffle.file.buffer，64)      // 默认32k  每次扩大一倍，看看效果。
set(spark.shuffle.memoryFraction，0.2)  // 每次提高0.1，看看效果。
```

很多资料、网上视频，都会说，这两个参数，是调节shuffle性能的不二选择，很有效果的样子，实际上，不是这样的。以实际的生产经验来说，这两个参数没有那么重要，往往来说，shuffle的性能不是因为这方面的原因导致的。


### 3.6.4 HashShuffleManager与SortShuffleManager

**上面我们所讲shuffle原理是指HashShuffleManager**，其实是一种比较老旧的shuffle manager，从spark 1.2.x版本以后，默认的是SortShuffleManager。
之前讲解的一些调优的点，比如consolidateFiles机制、map端缓冲、reduce端内存占比。这些对任何shuffle manager都是有用的。

在spark 1.5.x后，又出来了一种tungsten-sort shuffleMnager。效果跟sort shuffle manager是差不多的。
唯一的不同之处在于，钨丝manager，是使用了自己实现的一套内存管理机制，性能上有很大的提升， 而且可以避免shuffle过程中产生的大量的OOM，GC，等等内存相关的异常。

SortShuffleManager与HashShuffleManager两点不同：

1、SortShuffleManager会对每个reduce task要处理的数据，进行排序（默认的）。
2、SortShuffleManager会避免像HashShuffleManager那样，默认就去创建多份磁盘文件。一个task，只会写入一个磁盘文件，不同reduce task的数据，用offset来划分界定。


**hash、sort、tungsten-sort。如何来选择？**
1、需不需要数据默认就让spark给你进行排序？就好像mapreduce，默认就是有按照key的排序。如果不需要的话，其实还是建议搭建就使用最基本的HashShuffleManager，因为最开始就是考虑的是不排序，换取高性能；

2、什么时候需要用sort shuffle manager？如果你需要你的那些数据按key排序了，那么就选择这种吧，而且要注意，reduce task的数量应该是超过200的，这样sort、merge（多个文件合并成一个）的机制，才能生效把。但是这里要注意，你一定要自己考量一下，有没有必要在shuffle的过程中，就做这个事情，毕竟对性能是有影响的。

3、如果你不需要排序，而且你希望你的每个task输出的文件最终是会合并成一份的，你自己认为可以减少性能开销；可以去调节bypassMergeThreshold这个阈值，比如你的reduce task数量是500，默认阈值是200，所以默认还是会进行sort和直接merge的；可以将阈值调节成550，不会进行sort，按照hash的做法，每个reduce task创建一份输出文件，最后合并成一份文件。（一定要提醒大家，这个参数，其实我们通常不会在生产环境里去使用，也没有经过验证说，这样的方式，到底有多少性能的提升）

4、如果你想选用sort based shuffle manager，而且你们公司的spark版本比较高，是1.5.x版本的，那么可以考虑去尝试使用tungsten-sort shuffle manager。看看性能的提升与稳定性怎么样。（唉，开源出来的项目都是落后了快五年了的）

总结：
1、在生产环境中，不建议大家贸然使用第三点和第四点：
2、如果你不想要你的数据在shuffle时排序，那么就自己设置一下，用hash shuffle manager。
3、如果你的确是需要你的数据在shuffle时进行排序的，那么就默认不用动，默认就是sort shuffle manager；或者是什么？如果你压根儿不care是否排序这个事儿，那么就默认让他就是sort的。调节一些其他的参数（consolidation机制）。（80%，都是用这种）

```
new SparkConf().set("spark.shuffle.manager", "hash")  // hash、tungsten-sort、默认为sort
new SparkConf().set("spark.shuffle.sort.bypassMergeThreshold", "550")   // 默认200
```
当reduce task数量少于等于200；map task创建的输出文件小于等于200的；会将所有的输出文件合并为一份文件。且不进行sort排序，节省了性能开销。





