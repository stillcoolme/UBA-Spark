## 性能调优

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
* 一个Stage内，最终的RDD有多少个partition，就会产生多少个task；一个task处理一个partition的数据；
* BlockManager负责Executor，task的数据管理，task来它这里拿数据；

## 1.1 资源分配
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

## 1.2 调节并行度

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


## 1.3 重构RDD架构以及RDD持久化

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


## 1.4 大变量进行广播， 使用Kryo序列化， 本地化等待时间

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



## 1.5 JVM调优

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



## 1.6 Suffle调优

### 1.6.1 shuffle流程
shuffle，一定是分为两个stage来完成的。因为这其实是个逆向的过程，不是stage决定shuffle，是shuffle决定stage。
在某个action触发job的时候，DAGScheduler会负责划分job为多个stage。划分的依据，就是如果发现有会触发shuffle操作的算子（reduceByKey）。
就将这个操作的前半部分，以及之前所有的RDD和transformation操作，划分为一个stage；
shuffle操作的后半部分，以及后面的，直到action为止的RDD和transformation操作，划分为另外一个stage。

每一个shuffle的前半部分stage的task，**每个task**都会**创建下一个stage的task数量相同的文件**，比如下一个stage会有10个task，那么当前stage每个task都会创建10份文件；会将同一个key对应的values，写入同一个文件中的；
不同节点上的task，也一定会将同一个key对应的values，写入下一个stage的同一个task对应的文件中。

shuffle的后半部分stage的task，每个task都会从各个节点上的task写的属于自己的那一份文件中，拉取key, value对；
然后task会有一个内存缓冲区，然后会用HashMap，进行key, values的汇聚；
task会用我们自己定义的聚合函数reduceByKey(_+_)，把所有values进行一对一的累加；聚合出来最终的值。


### 1.6.2 合并map端输出文件

```sparConf().set("spark.shuffle.consolidateFiles", "true")
```

开启了map端输出文件的合并机制之后：

第一个stage，同时运行cpu core个task，比如cpu core是2个，并行运行2个task；每个task都创建下一个stage的task数量个文件；

第一个stage，并行运行的2个task执行完以后；就会执行另外两个task；**另外2个task不会再重新创建输出文件；而是复用之前的task创建的map端输出文件，将数据写入上一批task的输出文件中**。

第二个stage，task在拉取数据的时候，就不会去拉取上一个stage每一个task为自己创建的那份输出文件了；而是拉取少量的输出文件，每个输出文件中，可能包含了多个task给自己的map端输出。


### 1.6.3 调节map端内存缓冲与reduce端内存占比
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


### 1.6.4 HashShuffleManager与SortShuffleManager

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



## 1.7 算子调优

### 1.7.1 MapPartitions提升Map类操作性能
普通的mapToPair，当一个partition中有1万条数据，function要执行和计算1万次。
但是，使用MapPartitions操作之后，一个task仅仅会执行一次function，function一次接收所有的partition数据。只要执行一次就可以了，性能比较高。
但是MapPartitions操作，对于大量数据来说，一次传入一个function以后，那么可能一下子内存不够，但是又没有办法去腾出内存空间来，可能就OOM。

在项目中，自己先去估算一下RDD的数据量，以及每个partition的量，还有自己分配给每个executor的内存资源。看看一下子内存容纳所有的partition数据，行不行。如果行，可以试一下，能跑通就好。性能肯定是有提升的。

### 1.7.2 使用coalesce减少分区数量
RDD这种filter之后，RDD中的每个partition的数据量，可能都不太一样了。
问题：
1、每个partition数据量变少了，但是在后面进行处理的时候，还跟partition数量一样数量的task，来进行处理；有点浪费task计算资源。
2、每个partition的数据量不一样，会导致后面的每个task处理每个partition的时候，每个task要处理的数据量就不同，处理速度相差大，导致数据倾斜。。。。

针对上述的两个问题，能够怎么解决呢？

1、针对第一个问题，希望可以进行partition的压缩吧，因为数据量变少了，那么partition其实也完全可以对应的变少。比如原来是4个partition，现在完全可以变成2个partition。那么就只要用后面的2个task来处理即可。就不会造成task计算资源的浪费。

2、针对第二个问题，其实解决方案跟第一个问题是一样的；也是去压缩partition，尽量让每个partition的数据量差不多。那么这样的话，后面的task分配到的partition的数据量也就差不多。不会造成有的task运行速度特别慢，有的task运行速度特别快。避免了数据倾斜的问题。

主要就是用于在filter操作之后，添加coalesce算子，针对每个partition的数据量各不相同的情况，来压缩partition的数量。减少partition的数量，而且让每个partition的数据量都尽量均匀紧凑。


### 1.7.3 foreachPartition优化写数据库性能
默认的foreach的性能缺陷在哪里？
1. 对于每条数据，都要单独去调用一次function，task为每个数据都要去执行一次function函数。如果100万条数据，（一个partition），调用100万次。性能比较差。
2. 浪费数据库连接资源。

在生产环境中，都使用foreachPartition来写数据库
1、对于我们写的function函数，就调用一次，一次传入一个partition所有的数据，但是太多也容易OOM。
2、主要创建或者获取一个数据库连接就可以
3、只要向数据库发送一次SQL语句和多组参数即可

**local模式跑的时候foreachPartition批量入库会卡住**，可能资源不足，因为用standalone集群跑的时候不会出现。

### 1.7.4 repartition 解决Spark SQL低并行度的性能问题
并行度：可以这样调节：
1、spark.default.parallelism   指定为全部executor的cpu core总数的2~3倍
2、textFile()，传入第二个参数，指定partition数量（比较少用）
**但是通过spark.default.parallelism参数指定的并行度，只会在没有Spark SQL的stage中生效**。
Spark SQL自己会默认根据hive表对应的hdfs文件的block，自动设置Spark SQL查询所在的那个stage的并行度。

比如第一个stage，用了Spark SQL从hive表中查询出了一些数据，然后做了一些transformation操作，接着做了一个shuffle操作（groupByKey），这些都不会应用指定的并行度可能会有非常复杂的业务逻辑和算法，就会导致第一个stage的速度，特别慢；下一个stage，在shuffle操作之后，做了一些transformation操作才会变成你自己设置的那个并行度。

解决上述Spark SQL无法设置并行度和task数量的办法，是什么呢？

repartition算子，可以将你用Spark SQL查询出来的RDD，使用repartition算子时，去重新进行分区，此时可以分区成多个partition，比如从20个partition分区成100个。
就可以避免跟Spark SQL绑定在一个stage中的算子，只能使用少量的task去处理大量数据以及复杂的算法逻辑。

### 1.7.5 reduceByKey本地聚合介绍
reduceByKey，相较于普通的shuffle操作（比如groupByKey），它有一个特点：会进行map端的本地聚合。
对map端给下个stage每个task创建的输出文件中，写数据之前，就会进行本地的combiner操作，也就是说对每一个key，对应的values，都会执行你的算子函数。

用reduceByKey对性能的提升：
1、在本地进行聚合以后，在map端的数据量就变少了，减少磁盘IO。而且可以减少磁盘空间的占用。
2、下一个stage，拉取数据的量，也就变少了。减少网络的数据传输的性能消耗。
3、在reduce端进行数据缓存的内存占用变少了，要进行聚合的数据量也变少了。

reduceByKey在什么情况下使用呢？
1、简单的wordcount程序。
2、对于一些类似于要对每个key进行一些字符串拼接的这种较为复杂的操作，可以自己衡量一下，其实有时，也是可以使用reduceByKey来实现的。但是不太好实现。如果真能够实现出来，对性能绝对是有帮助的。

## 1.8 troubleshooting调优

### 1.8.1 控制shuffle reduce端缓冲大小以避免OOM
map端的task是不断的输出数据的，数据量可能是很大的。
但是，在map端写过来一点数据，reduce端task就会拉取一小部分数据，先放在buffer中，立即进行后面的聚合、算子函数的应用。
每次reduece能够拉取多少数据，就由buffer来决定。然后才用后面的executor分配的堆内存占比（0.2），hashmap，去进行后续的聚合、函数的执行。

reduce端缓冲默认是48MB（buffer），可能会出什么问题？
缓冲达到最大极限值，再加上你的reduce端执行的聚合函数的代码，可能会创建大量的对象。reduce端的内存中，就会发生内存溢出的问题。
这个时候，就应该减少reduce端task缓冲的大小。我宁愿多拉取几次，但是每次同时能够拉取到reduce端每个task的数量，比较少，就不容易发生OOM内存溢出的问题。

另外，如果你的Map端输出的数据量也不是特别大，然后你的**整个application的资源也特别充足，就可以尝试去增加这个reduce端缓冲大小的，比如从48M，变成96M**。
这样每次reduce task能够拉取的数据量就很大。需要拉取的次数也就变少了。
最终达到的效果，就应该是性能上的一定程度上的提升。
设置
spark.reducer.maxSizeInFlight


### 1.8.2 解决JVM GC导致的shuffle文件拉取失败
比如，executor的JVM进程，内存不够了，发生GC，导致BlockManger,netty通信都停了。
下一个stage的executor，可能是还没有停止掉的，task想要去上一个stage的task所在的exeuctor，去拉取属于自己的数据，结果由于对方正在GC，就导致拉取了半天没有拉取到。
可能会报错shuffle file not found。但是，可能下一个stage又重新提交了stage或task以后，再执行就没有问题了，因为可能第二次就没有碰到JVM在gc了。
有的时候，出现这种情况以后，会重新去提交stage、task。重新执行一遍，发现就好了。没有这种错误了。


spark.shuffle.io.maxRetries 3
spark.shuffle.io.retryWait 5s

针对这种情况，我们完全可以进行预备性的参数调节。
增大上述两个参数的值，达到比较大的一个值，尽量保证第二个stage的task，一定能够拉取到上一个stage的输出文件。
尽量避免因为gc导致的shuffle file not found，无法拉取到的问题。


### 1.8.3 yarn-cluster模式的JVM内存溢出无法执行问题
总结一下yarn-client和yarn-cluster模式的不同之处：
yarn-client模式，driver运行在本地机器上的；yarn-cluster模式，driver是运行在yarn集群上某个nodemanager节点上面的。
yarn-client的driver运行在本地，通常来说本地机器跟yarn集群都不会在一个机房的，所以说性能可能不是特别好；yarn-cluster模式下，driver是跟yarn集群运行在一个机房内，性能上来说，也会好一些。

实践经验，碰到的yarn-cluster的问题：

有的时候，运行一些包含了spark sql的spark作业，可能会碰到yarn-client模式下，可以正常提交运行；yarn-cluster模式下，可能是无法提交运行的，会报出JVM的PermGen（永久代）的内存溢出，会报出PermGen Out of Memory error log。

yarn-client模式下，driver是运行在本地机器上的，spark使用的JVM的PermGen的配置，是本地的spark-class文件（spark客户端是默认有配置的），JVM的永久代的大小是128M，这个是没有问题的；但是在yarn-cluster模式下，driver是运行在yarn集群的某个节点上的，使用的是没有经过配置的默认设置（PermGen永久代大小），82M。

spark-submit脚本中，加入以下配置即可：
--conf spark.driver.extraJavaOptions="-XX:PermSize=128M -XX:MaxPermSize=256M"

另外，sql有大量的or语句。可能就会出现一个driver端的jvm stack overflow。
基本上就是由于调用的方法层级过多，因为产生了非常深的，超出了JVM栈深度限制的，递归。spark sql内部源码中，在解析sqlor特别多的话，就会发生大量的递归。
建议不要搞那么复杂的spark sql语句。
采用替代方案：将一条sql语句，拆解成多条sql语句来执行。
每条sql语句，就只有100个or子句以内；一条一条SQL语句来执行。



## 1.9 数据倾斜条调优
### 1.9.1 数据倾斜的原理、现象、产生原因与定位
**原因**
在执行shuffle操作的时候，是按照key，来进行values的数据的输出、拉取和聚合的。
同一个key的values，一定是分配到一个reduce task进行处理的。
假如多个key对应的values，总共是90万。
可能某个key对应了88万数据，key-88万values，分配到一个task上去面去执行，执行很慢。而另外两个task，可能各分配到了1万数据，可能是数百个key，才对应的1万条数据。
就出现数据倾斜。
基本只可能是因为发生了shuffle操作，出现数据倾斜的问题。

定位：在自己的程序里面找找，哪些地方用了会产生shuffle的算子，groupByKey、countByKey、reduceByKey、join。看log，看看是执行到了第几个stage。哪一个stage，task特别慢，就能够自己用肉眼去对你的spark代码进行stage的划分，就能够通过stage定位到你的代码，哪里发生了数据倾斜。

**第一个方案：聚合源数据**
做一些聚合的操作：groupByKey、reduceByKey，说白了就是对每个key对应的values执行一定的计算。
spark作业的数据来源如果是hive，可以直接在生成hive表的hive etl中，对数据进行聚合。
比如按key来分组，将key对应的所有的values，全部用一种特殊的格式，拼接到一个字符串里面去，每个key就只对应一条数据。比如
```
key=sessionid, value: action_seq=1|user_id=1|search_keyword=火锅|category_id=001;action_seq=2|user_id=1|search_keyword=涮肉|category_id=001”。

然后在spark中，拿到key=sessionid，values<Iterable>。

```
或者在hive里面就进行reduceByKey计算。
spark中就不需要再去执行groupByKey+map这种操作了。
直接对每个key对应的values字符串进行map进行你需要的操作即可。也就根本不可能导致数据倾斜。

具体怎么去在hive etl中聚合和操作，就得根据你碰到数据倾斜问题的时候，你的spark作业的源hive表的具体情况，具体需求，具体功能，具体分析。

具体对于我们的程序来说，完全可以将aggregateBySession()这一步操作，放在一个hive etl中来做，形成一个新的表。
对每天的用户访问行为数据，都按session粒度进行聚合，写一个hive sql。
**在spark程序中，就不要去做groupByKey+mapToPair这种算子了**。
直接从当天的session聚合表中，用SparkSQL查询出来对应的数据，即可。
这个RDD在后面就可以使用了。


**第二个方案：过滤导致倾斜的key**
如果你能够接受某些数据，在spark作业中直接就摒弃掉，不使用。
比如说，总共有100万个key。只有2个key，是数据量达到10万的。其他所有的key，对应的数量都是几十。
这个时候，你自己可以去取舍，如果业务和需求可以理解和接受的话，在你从hive表查询源数据的时候，直接在sql中用where条件，过滤掉某几个key。
那么这几个原先有大量数据，会导致数据倾斜的key，被过滤掉之后，那么在你的spark作业中，自然就不会发生数据倾斜了。


### 1.9.2 提高shuffle操作的reduce并行度
将reduce task的数量，变多，就可以让每个reduce task分配到更少的数据量。
这样的话，也许就可以缓解，或者甚至是基本解决掉数据倾斜的问题。
提升shuffle reduce端并行度，怎么来操作？
在调用shuffle算子的时候，传入进去一个参数。就代表了那个shuffle操作的reduce端的并行度。那么在进行shuffle操作的时候，就会对应着创建指定数量的reduce task。
按照log，找到发生数据倾斜的shuffle操作，给它传入一个并行度数字，这样的话，原先那个task分配到的数据，肯定会变少。就至少可以避免OOM的情况，程序至少是可以跑的。

但是**没有从根本上改变数据倾斜的本质和问题。**
不像第一个和第二个方案（直接避免了数据倾斜的发生）。
原理没有改变，只是说，尽可能地去缓解和减轻shuffle reduce task的数据压力，以及数据倾斜的问题。

实际生产环境中的经验。
1、如果最理想的情况下，提升并行度以后，减轻了数据倾斜的问题，那么就最好。就不用做其他的数据倾斜解决方案了。
2、不太理想的情况下，就是比如之前某个task运行特别慢，要5个小时，现在稍微快了一点，变成了4个小时；或者是原先运行到某个task，直接OOM，现在至少不会OOM了，但是那个task运行特别慢，要5个小时才能跑完。
那么，如果出现第二种情况的话，各位，就立即放弃第三种方案，开始去尝试和选择后面的方案。


### 1.9.3 使用随机key实现双重聚合
1、原理
第一轮聚合的时候，对key进行打散，将原先一样的key，变成不一样的key，相当于是将每个key分为多组；比如原来是
```
(5,44)、(6,45)、(7,45)
就可以对key添加一个随机数
(1_5,44)、(3_6,45)、(2_7,45)
针对多个组，进行key的局部聚合；
接着，再去除掉每个key的前缀，恢复成
(5,44)、(6,45)、(7,45)
然后对所有的key，进行全局的聚合。
```
对groupByKey、reduceByKey造成的数据倾斜，有比较好的效果。

2、使用场景
（1）groupByKey
（2）reduceByKey

### 1.9.4 将导致倾斜的key单独进行join
这个方案关键之处在于: 
将发生数据倾斜的key，单独拉出来，放到一个RDD中去；
用这个原本会倾斜的key RDD跟其他RDD，单独去join一下，
key对应的数据，可能就会分散到多个task中去进行join操作。
这个key跟之前其他的key混合在一个RDD中时，肯定是会导致一个key对应的所有数据，都到一个task中去，就会导致数据倾斜。

**这种方案什么时候适合使用？**

针对你的RDD的数据，你可以自己把它转换成一个中间表，或者是直接用countByKey()的方式，你可以看一下这个RDD各个key对应的数据量；
RDD有一个或少数几个key，是对应的数据量特别多；
此时可以采用咱们的这种方案，单拉出来那个最多的key；
单独进行join，尽可能地将key分散到各个task上去进行join操作。

### 1.9.5 使用随机数以及扩容表进行join
这个方案是没办法彻底解决数据倾斜的，更多的，是一种对数据倾斜的缓解。
局限性：
1、因为join两个RDD都很大，就没有办法去将某一个RDD扩的特别大，一般是10倍。
2、如果就是10倍的话，那么数据倾斜问题，只能说是缓解和减轻，不能说彻底解决。

步骤：
1、选择一个RDD，要用flatMap，进行扩容，将每条数据，映射为多条数据，每个映射出来的数据，都带了一个n以内的随机数，通常来说，会选择10。
2、将另外一个RDD，做普通的map映射操作，每条数据，都打上一个10以内的随机数。
3、最后，将两个处理后的RDD，进行join操作。
4、join完以后，可以执行map操作，去将之前打上的随机数，给去掉，然后再和另外一个普通RDD join以后的结果，进行union操作。

sample采样倾斜key并单独进行join
将key，从另外一个RDD中过滤出的数据，可能只有一条，或者几条，此时，咱们可以任意进行扩容，扩成1000倍。
将从第一个RDD中拆分出来的那个倾斜key RDD，打上1000以内的一个随机数。
打散成100份，甚至1000份去进行join，那么就肯定没有数据倾斜的问题了吧。
这种情况下，还可以配合上，提升shuffle reduce并行度，join(rdd, 1000)。

