package com.stillcoolme.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class MockRealTimeData extends Thread {

	private static Logger LOG = LoggerFactory.getLogger(MockRealTimeData.class);
	private static final String TOPIC_NAME = "ad";
	private static final String BROKER = "localhost:9092";
	//private static final String BROKER = "gosearch-node1:9092,gosearch-node2:9092,gosearch-node3:9092";
	private static final Random random = new Random();
	private static final String[] provinces = new String[]{"Jiangsu", "Hubei", "Hunan", "Henan", "Hebei"};  
	private static final Map<String, String[]> provinceCityMap = new HashMap<String, String[]>();
	private Producer<Integer, String> producer;
	
	public MockRealTimeData() {
		Properties props = new Properties();
		props.put("bootstrap.servers", BROKER);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);

		provinceCityMap.put("Jiangsu", new String[] {"Nanjing", "Suzhou"});  
		provinceCityMap.put("Hubei", new String[] {"Wuhan", "Jingzhou"});
		provinceCityMap.put("Hunan", new String[] {"Changsha", "Xiangtan"});
		provinceCityMap.put("Henan", new String[] {"Zhengzhou", "Luoyang"});
		provinceCityMap.put("Hebei", new String[] {"Shijiazhuang", "Tangshan"});
	}

	public void run() {
		while(true) {	
			String province = provinces[random.nextInt(5)];  
			String city = provinceCityMap.get(province)[random.nextInt(2)];
			String log = new Date().getTime() + " " + province + " " + city + " " 
					+ random.nextInt(1000) + " " + random.nextInt(10);  
			producer.send(new ProducerRecord<>(TOPIC_NAME, log));
			System.out.println("produce log: " + log);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}  
		}
	}
	
	/**
	 * 启动Kafka Producer
	 * @param args
	 */
	public static void main(String[] args) {
		MockRealTimeData producer = new MockRealTimeData();
		producer.start();
	}
	
}
