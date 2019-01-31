## 积累一些使用spark过程中的

* 使用RowFactory来建row，做一些rdd到row的转换工作
```java
// Convert records of the RDD (people) to Rows
JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
  String[] attributes = record.split(",");
  return RowFactory.create(attributes[0], attributes[1].trim());
});
```

* 两个数据集求并集，一个设为+1，一个设为-1

最近周期内相对之前一个周期访问次数增长最快的10个用户。

解决思路：上一周期的每次访问设为 -1， 现在的这一周期的每次访问设为 +1。
然后相同用户的相加，就得到这一周期的用户访问相比上一周期多出多少次。