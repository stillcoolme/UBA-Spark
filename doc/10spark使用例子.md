## 积累一些使用spark过程中的
* 使用RowFactory来建row，做一些row的转换工作
```java
// Convert records of the RDD (people) to Rows
JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
  String[] attributes = record.split(",");
  return RowFactory.create(attributes[0], attributes[1].trim());
});
```

