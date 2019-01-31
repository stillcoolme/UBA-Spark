用户活跃度分析的两份数据: user_action_log.json、user_base_info.json

root
 |-- _corrupt_record: string (nullable = true)    ??
 |-- actionTime: string (nullable = true)
 |-- actionType: long (nullable = true)
 |-- logId: long (nullable = true)
 |-- purchaseMoney: double (nullable = true)
 |-- userId: long (nullable = true)

root
 |-- userId: long (nullable = true)
 |-- username: string (nullable = true)


读取user_action_log.json报warn，The corrupted record exists in column _corrupt_record。
查看读出来的df发现所有字段为空，都在_corrupt_record字段里面了。作业居然也能运行，但是结果错了。
检查json格式是因为
{"logId": 02,"userId": 0, "actionTime": "2018-11-18 15:42:45", "actionType": 0, "purchaseMoney": 0.0}
logId的值多了个0在见面，改成"logId": 2 即可。
