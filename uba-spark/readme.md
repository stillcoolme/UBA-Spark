## 1. 本项目功能
用户在网站内的访问过程，就称之为一次session。简单理解，session就是某一天某一个时间段内，某个用户对网站从打开/进入，到做了大量操作，到最后关闭浏览器。的过程。就叫做session。
1、对用户访问session进行分析，筛选出指定的一些用户（有特定年龄、职业、城市）
2、JDBC辅助类封装
3、用户在指定日期范围内访问session聚合统计，比如，统计出访问时长在0~3s的session占总session数量的比例
4、按时间比例随机抽取session。比如一天有24个小时，其中12:00~13:00的session数量占当天总session数量的50%，当天总session数量是10000个，那么当天总共要抽取1000个session，12:00~13:00的用户，就得抽取1000*50%=500。而且这500个需要随机抽取。
5、获取点击量、下单量和支付量都排名10的商品种类
6、获取top10的商品种类的点击数量排名前10的session
7、复杂性能调优全套解决方案
8、十亿级数据troubleshooting经验总结
9、数据倾斜全套完美解决方案
10、模块功能演示

## 2. 实际企业项目中的架构
1、J2EE的平台（美观的前端页面），通过这个J2EE平台可以让使用者，提交各种各样的分析任务，其中就包括一个模块，就是用户访问session分析模块；可以指定各种各样的筛选条件，比如年龄范围、职业、城市等等。。
2、J2EE平台接收到了执行统计分析任务的请求之后，会调用底层的封装了spark-submit的shell脚本（Runtime、Process），shell脚本进而提交我们编写的Spark作业。
3、Spark作业获取使用者指定的筛选参数，然后运行复杂的作业逻辑，进行该模块的统计和分析。
4、Spark作业统计和分析的结果，会写入MySQL中，指定的表
5、最后，J2EE平台，使用者可以通过前端页面（美观），以表格、图表的形式展示和查看MySQL中存储的该统计分析任务的结果数据。
（讲师本人在实际企业中，是做大数据平台的，所以上面一整套，除了前端页面不用我做，当然了，偶尔可能也需要做一些前端，从J2EE到Spark到MySQL的性能调优，实际上都是我的工作。

项目流程：
!(项目流程图)[https://img2018.cnblogs.com/blog/659358/201810/659358-20181022224756821-1143146095.png]

## 3. 需求分析
功能弄清楚了，写代码的时候心里才有底。。。

1、按条件筛选session
搜索过某些关键词的用户、访问时间在某个时间段内的用户、年龄在某个范围内的用户、职业在某个范围内的用户、所在某个城市的用户，发起的session。找到对应的这些用户的session，也就是我们所说的第一步，按条件筛选session。

这个功能，就最大的作用就是灵活。也就是说，可以让使用者，对感兴趣的和关系的用户群体，进行后续各种复杂业务逻辑的统计和分析，那么拿到的结果数据，就是只是针对特殊用户群体的分析结果；而不是对所有用户进行分析的泛泛的分析结果。比如说，现在某个企业高层，就是想看到用户群体中，28~35岁的，老师职业的群体，对应的一些统计和分析的结果数据，从而辅助高管进行公司战略上的决策制定。

思路：
首先提出第一个问题，你要按条件筛选session，但是这个筛选的粒度是不同的，比如说搜索词、访问时间，那么这个都是session粒度的；针对用户的基础信息进行筛选，年龄、性别、职业，则是用户粒度的；所以说筛选粒度是不统一的。
第二个问题，我们的每天的用户访问数据量是很大的，user_visit_action这个表，一行就代表了用户的一个行为，比如点击或者搜索；如果每天的活跃用户数量在千万级别的话。那么这个表，每天的数据量大概在至少5亿以上，在10亿左右。

那么针对这个筛选粒度不统一的问题，以及数据量巨大（10亿/day），可能会有两个问题；首先第一个，就是，如果不统一筛选粒度的话，那么就必须得对所有的数据进行全量扫描；第二个，就是全量扫描的话，量实在太大了，一天如果在10亿左右，那么10天呢（100亿），100呢，1000亿。量太大的话，会导致Spark作业的运行速度大幅度降低。极大的影响平台使用者的用户体验。

所以为了解决这个问题，我们对原始的数据，进行session粒度的聚合。用一些最基本的筛选条件，比如时间范围，从hive表中提取数据，按照session_id这个字段进行聚合，那么聚合后的一条记录，就是一个用户的某个session在指定时间内的访问的记录，比如搜索过的所有的关键词、点击过的所有的品类id、session对应的userid关联的用户的基础信息。

聚合过后，针对session粒度的数据，按照使用者指定的筛选条件，进行数据的筛选。筛选出来符合条件的用session粒度的数据。其实就是我们想要的那些session了。

2、统计出符合条件的session中，访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、10m~30m、30m以上各个范围内的session占比；访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个范围内的session占比

session访问时长，也就是说一个session对应的开始的action，到结束的action，之间的时间范围；还有，就是访问步长，指的是，一个session执行期间内，依次点击过多少个页面，比如说，一次session，维持了1分钟，那么访问时长就是1m，然后在这1分钟内，点击了10个页面，那么session的访问步长，就是10.

比如说，符合第一步筛选出来的session的数量大概是有1000万个。那么里面，我们要计算出，访问时长在1s~3s内的session的数量，并除以符合条件的总session数量（比如1000万），比如是100万/1000万，那么1s~3s内的session占比就是10%。依次类推，这里说的统计，就是这个意思。

这个功能的作用，可以让人从全局的角度看到，符合某些条件的用户群体，使用我们的产品的一些习惯。比如大多数人，到底是会在产品中停留多长时间，大多数人，会在一次使用产品的过程中，访问多少个页面。那么对于使用者来说，有一个全局和清晰的认识。
思路：
首先要明确，我们的spark作业是分布式的。每个spark task在执行我们的统计逻辑的时候，可能就需要对一个全局的变量，进行累加操作。比如代表访问时长在1s~3s的session数量，初始是0。分布式处理所有的session，判断每个session的访问时长，如果是1s~3s内的话，那么就给1s~3s内的session计数器，累加1。

那么在spark中，要实现分布式安全的累加操作，基本上只有一个最好的选择，就是Accumulator变量。但是，如果是基础的Accumulator变量，那么可能需要将近20个Accumulator变量，1s~3s、4s~6s。。；但是这样的话，就会导致代码中充斥了大量的Accumulator变量，导致维护变得更加复杂，在修改代码的时候，很可能会导致错误。比如说判断出一个session访问时长在4s~6s，但是代码中不小心写了一个bug（由于Accumulator太多了），比如说，更新了1s~3s的范围的Accumulator变量。导致统计出错。
所以，对于这个情况，那么我们就可以使用自定义Accumulator的技术，来实现复杂的分布式计算。也就是说，就用一个Accumulator，来计算所有的指标。

3、在符合条件的session中，按照时间比例随机抽取1000个session

这个按照时间比例是什么意思呢？随机抽取本身是很简单的，但是按照时间比例，就很复杂了。比如说，这一天总共有1000万的session。那么我现在总共要从这1000万session中，随机抽取出来1000个session。但是这个随机不是那么简单的。需要做到如下几点要求：首先，如果这一天的12:00~13:00的session数量是100万，那么这个小时的session占比就是1/10，那么这个小时中的100万的session，我们就要抽取1/10 * 1000 = 100个。然后再从这个小时的100万session中，随机抽取出100个session。以此类推，其他小时的抽取也是这样做。
这个功能的作用，是说，可以让使用者，能够对于符合条件的session，按照时间比例均匀的随机采样出1000个session，然后观察每个session具体的点击流/行为，比如先进入了首页、然后点击了食品品类、然后点击了雨润火腿肠商品、然后搜索了火腿肠罐头的关键词、接着对王中王火腿肠下了订单、最后对订单做了支付。
之所以要做到按时间比例随机采用抽取，就是要做到，观察样本的公平性。
思路：
需求上已经明确了。那么剩下的就是具体的实现了。具体的实现这里不多说，技术上来说，就是要综合运用Spark的countByKey、groupByKey、mapToPair等算子，来开发一个复杂的按时间比例随机均匀采样抽取的算法。（大数据算法）


4、在符合条件的session中，获取点击、下单和支付数量排名前10的品类。
对于这些session，每个session可能都会对一些品类的商品进行点击、下单和支付等等行为。那么现在就需要获取这些session点击、下单和支付数量排名前10的最热门的品类。也就是说，要计算出所有这些session对各个品类的点击、下单和支付的次数，然后按照这三个属性进行排序，获取前10个品类。
这个需求是很有意义的，因为这样，就可以让数据分析师、产品经理、公司高层，随时随地都可以看到自己感兴趣的那一批用户最喜欢的10个品类，清晰地了解到不同层次、不同类型的用户的心理和喜好，从而对自己公司和产品的定位有清晰的了解，并且可以更加深入的了解自己的用户，更好的调整公司战略。
思路：
使用Spark的自定义Key二次排序算法的技术，来实现所有品类，按照三个字段，点击数量、下单数量、支付数量依次进行排序，首先比较点击数量，如果相同的话，那么比较下单数量，如果还是相同，那么比较支付数量。


5、对于排名前10的品类，分别获取其点击次数排名前10的session
这个就是说，对于top10的品类，每一个都要获取对它点击次数排名前10的session。
这个功能，可以让我们看到，对某个用户群体最感兴趣的品类，各个品类最感兴趣最典型的用户的session的行为。
思路：
这个需求，需要使用Spark的分组取TopN的算法来进行实现。也就是说对排名前10的品类对应的数据，按照品类id进行分组，然后求出每组点击数量排名前10的session。

学习到：
1、通过底层数据聚合，来减少spark作业处理数据量，从而提升spark作业的性能（从根本上提升spark性能的技巧）
2、自定义Accumulator实现复杂分布式计算的技术
3、Spark按时间比例随机抽取算法
4、Spark自定义key二次排序技术
5、Spark分组取TopN算法
6、通过Spark的各种功能和技术点，进行各种聚合、采样、排序、取TopN业务的实现

## 4. 表结构设计
数据设计，往往包含两个环节：1.我们的上游数据，数据调研环节看到的项目需要的基础数据，是否要针对其开发一些Hive ETL，对数据进行进一步的处理和转换，从而让我们能够更加方便的和快速的去计算和执行spark作业；2. 设计spark作业要保存结果数据的业务表的结构，从而让J2EE平台可以使用业务表中的数据，来为使用者展示任务执行结果。
在本项目中，我们所有的数据设计环节，只会涉及第二个，不会涉及第一个。不要花时间去做Hive ETL了。设计MySQL中的业务表的结构。

第一表：session_aggr_stat表，存储第一个功能，session聚合统计的结果
```
CREATE TABLE `session_aggr_stat` (
  `task_id` int(11) NOT NULL,
  `session_count` int(11) DEFAULT NULL,
  `1s_3s` double DEFAULT NULL,
  `4s_6s` double DEFAULT NULL,
  `7s_9s` double DEFAULT NULL,
  `10s_30s` double DEFAULT NULL,
  `30s_60s` double DEFAULT NULL,
  `1m_3m` double DEFAULT NULL,
  `3m_10m` double DEFAULT NULL,
  `10m_30m` double DEFAULT NULL,
  `30m` double DEFAULT NULL,
  `1_3` double DEFAULT NULL,
  `4_6` double DEFAULT NULL,
  `7_9` double DEFAULT NULL,
  `10_30` double DEFAULT NULL,
  `30_60` double DEFAULT NULL,
  `60` double DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
```
第二个表：session_random_extract表，存储我们的按时间比例随机抽取功能抽取出来的1000个session
```
CREATE TABLE `session_random_extract` (
  `task_id` int(11) NOT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `start_time` varchar(50) DEFAULT NULL,
  `end_time` varchar(50) DEFAULT NULL,
  `search_keywords` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
```
第三个表：top10_category表，存储按点击、下单和支付排序出来的top10品类数据
```
CREATE TABLE `top10_category` (
  `task_id` int(11) NOT NULL,
  `category_id` int(11) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL,
  `order_count` int(11) DEFAULT NULL,
  `pay_count` int(11) DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
```
第四个表：top10_category_session表，存储top10每个品类的点击top10的session
```
CREATE TABLE `top10_category_session` (
  `task_id` int(11) NO NULL,
  `category_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
```
最后一张表：session_detail，用来存储随机抽取出来的session的明细数据、top10品类的session的明细数据
```
CREATE TABLE `session_detail` (
  `task_id` int(11) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `page_id` int(11) DEFAULT NULL,
  `action_time` varchar(255) DEFAULT NULL,
  `search_keyword` varchar(255) DEFAULT NULL,
  `click_category_id` int(11) DEFAULT NULL,
  `click_product_id` int(11) DEFAULT NULL,
  `order_category_ids` varchar(255) DEFAULT NULL,
  `order_product_ids` varchar(255) DEFAULT NULL,
  `pay_category_ids` varchar(255) DEFAULT NULL,
  `pay_product_ids` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
```
额外的一张表：task表，用来存储J2EE平台插入其中的任务的信息
```
CREATE TABLE `task` (
  `task_id` int(11) NOT NULL AUTO_INCREMENT,
  `task_name` varchar(255) DEFAULT NULL,
  `create_time` varchar(255) DEFAULT NULL,
  `start_time` varchar(255) DEFAULT NULL,
  `finish_time` varchar(255) DEFAULT NULL,
  `task_type` varchar(255) DEFAULT NULL,
  `task_status` varchar(255) DEFAULT NULL,
  `task_param` text,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8
```
在数据设计以后，就正式进入一个漫长的环节，就是编码实现阶段，coding阶段。在编码实现阶段，每开发完一个功能，其实都会走后续的两个环节，就是本地测试和生产环境测试。

## 5. 编码
### datasource 单例模式
### dao 工厂模式
如果没有工厂模式，可能会出现的问题：
ITaskDAO接口和TaskDAOImpl实现类；实现类是可能会更换的；那么，如果你就使用普通的方式来创建DAO，比如ITaskDAO taskDAO = new TaskDAOImpl()，那么后续，如果你的TaskDAO的实现类变更了，那么你就必须在你的程序中，所有出现过TaskDAOImpl的地方，去更换掉这个实现类。这是非常非常麻烦的。
如果说，你的TaskDAOImpl这个类，在你的程序中出现了100次，那么你就需要修改100个地方。这对程序的维护是一场灾难。
工厂设计模式
对于一些种类的对象，使用一个工厂，来提供这些对象创建的方式，外界要使用某个类型的对象时，就直接通过工厂来获取即可。不用自己手动一个一个地方的去创建对应的对象。
那么，假使我们有100个地方用到了TaskDAOImpl。不需要去在100个地方都创建TaskDAOImpl()对象，只要在100个地方，都使用TaskFactory.getTaskDAO()方法，获取出来ITaskDAO接口类型的对象即可。
如果后面，比如说MySQL迁移到Oracle，我们重新开发了一套TaskDAOImpl实现类，那么就直接在工厂方法中，更换掉这个类即可。不需要再所有使用到的地方都去修改。

恩，我就来使用spring吧

### json 
json本质：一个对象实体就是键值对，用 {}。多个数据实体就是数组形式，用 []。

JSON是起到了什么作用呢？我们在task表中的task_param字段，会存放不同类型的任务对应的参数。比如说，用户访问session分析模块与页面单跳转化率统计模块的任务参数是不同的，但是，使用同一张task表来存储所有类型的任务。那么，你怎么来存储不同类型的任务的不同的参数呢？你的表的字段是事先要定好的呀。

所以，我们采取了，用一个task_param字段，来存储不同类型的任务的参数的方式。task_param字段中，实际上会存储一个任务所有的字段，使用JSON的格式封装所有任务参数，并存储在task_param字段中。就实现了非常灵活的方式。

如何来操作JSON格式的数据？

比如说，要获取JSON中某个字段的值。我们这里使用的是阿里的fastjson工具包。使用这个工具包，可以方便的将字符串类型的JSON数据，转换为一个JSONObject对象，然后通过其中的getX()方法，获取指定的字段的值。

## 10 遇到的问题

### 10.1 读取Long类型的空值报错
UserVisitSessionAnalyze类里面的
JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(）里面。
获取session的搜索行为 Long clickCategoryId = row.getLong(6); 读取 row的第7个字段是Long类型的。
如果为 Null 会报错。
看源码
```
private def getAnyValAs[T <: AnyVal](i: Int): T =
    if (isNullAt(i)) throw new NullPointerException(s"Value at index $i is null")
    else getAs[T](i)
```
先是做了是否为空的判断啊，所以我就直接调getAs不就好了。所以改为
Long clickCategoryId = row.getAs(6);

-------------
看博客上的想到上面的解决方法。。。
我遇到的两种情况吧
val DF = hc.sql("...............")
val rdd = DF.rdd.map(row => val label = row.getAs[Int]("age"))

1，如果getAs[Integer]("age")那么null值被拿出来依然为null
2，如果getAs[Int]("age")则 label = 0（本以为要报错的才对）

源码spark1.6
```
  /**
   * Returns the value of a given fieldName.
   * For primitive types if value is null it returns 'zero value' specific for primitive
   * ie. 0 for Int - use isNullAt to ensure that value is not null
   *
   * @throws UnsupportedOperationException when schema is not defined.
   * @throws IllegalArgumentException when fieldName do not exist.
   * @throws ClassCastException when data type does not match.
   */
  def getAs[T](fieldName: String): T = getAs[T](fieldIndex(fieldName))
```
建议:如果null不是你想的数据建议在SQL阶段就将其过滤掉

### 10.2 对象无法序列化

一口气写完了以后，一测老是报某个DaoImpl无法序列化。结果revert回去重新一点一点写，发现是其中的一个date没有取对值，结果居然会变成 某个DaoImpl无法序列化，真是不可思议。所以要写一点测一点。。。


### 10.3 spark1.x升级到spark2.x以及1.x和2.x的版本兼容
spark2.x中flatMapToPair的call返回的是Iterator，而不是Iterable接口了。
spark1.x
```
public static JavaRDD<String> workJob(JavaRDD<String> spark1Rdd) {
        JavaPairRDD<String, Integer> testRdd = spark1Rdd                .flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override            
            public Iterable<Tuple2<String, Integer>> call(String str)
                    throws Exception {
                ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();                
                return list;
            }
        });        
        return spark1Rdd;
    }
```

spark2.x
```
public static JavaRDD<String> workJob(JavaRDD<String> spark2Rdd) {
        JavaPairRDD<String, Integer> testRdd2 = spark2Rdd                .flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override            
            public Iterator<Tuple2<String, Integer>> call(String str)
                    throws Exception {
                ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();                
                return list.iterator();
            }
        });        
        return spark2Rdd;
    }
```
需要说明的是: 
上面的返回的rdd就直接用输入的 RDD显然是不合理的! 只是为了用最简洁的方式介绍代码的转换而已!

可以看到区别主要在于
1. spark 1.x中的Iterable对象 变成了spark2.x中的Iterator对象
2. 相应的,对于返回值为list的RDD,  spark2.x中要返回list.iterator();
还是很简单的吧。

问题在于 : 如果你有几个spark程序要运行在不同的环境下，有的现场用1.x,有的现场用2.x
你需要同时维护两种不同版本的spark，是不是耗时又耗力呢?

这个时候就需要考虑到 spark版本的兼容性,使你的程序能成功的运行在各种集群环境下。通过一个中间的接口来进行适配！！！
```
public class MyIterator<T> implements Iterator implements Iterable{
  private Iterator myIterable;

  public MyIterator(Iterable iterable)
    {
        myIterable = iterable.iterator();
    }

    @Override
    public boolean hasNext() 
    {
        return myIterable.hasNext();
    }

    @Override
    public Object next() 
    {
        return myIterable.next();
    }

    @Override
    public void remove() 
    {
        myIterable.remove();
    }

    @Override
    public Iterator iterator() 
    {
        return myIterable;
    }
}
```
只需要进行如上设计就可以实现版本的兼容了，那么应该如何应用呢?
```
JavaRDD<String> flatMapRDD = lines.flatMap(
  new FlatMapFunction<String, String>() {
    @Override
    public MyIterator<String> call(String s) throws Exception {
        String[] split = s.split("\\s+");
        // 这样就可以顺利接收spark1.x的Iterable接口对象了
        MyIterator myIterator = new MyIterator(Arrays.asList(split));
        return myIterator;
    }
});
```

## 11. 经验套路

### mapToPair形成自定义tuple
aggregateBySession() 的具体实现里面 三次用到了 mapToPair 算子！！！
用来调整数据结构、分组，形成Tuple2形式的 key,value。
一般的需求基本都要用到这个套路进行数据分组。


