package com.sears.kafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class DevKafkaSimpleProducer{

	 //String filePath = "C://workspaces//kafka-tutorial//kafka-simple-producer//src//main//resources//input.txt";
	
	public static void main(String[] args) {
		String filePath = "C://workspaces//kafka-tutorial//kafka-simple-producer//src//main//resources//input1.txt";
		Properties props = new Properties();

        props.put("metadata.broker.list", "rtckafka301p.dev.ch3.s.com:9092,rtckafka302p.dev.ch3.s.com:9092,rtckafka303p.dev.ch3.s.com:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new kafka.producer.ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);
       /* for(int i=0; i<=5;i++){
        	String key= i + "H";
        	KeyedMessage<String, String> keyedMessage = new KeyedMessage<>("SACHIN",key,"Arjun - "+i);
        	producer.send(keyedMessage);
        	System.out.println("Message Sent!!!");
        }*/
         String content=readFileByAllBytes(filePath);
         if(content!=null){
        	 KeyedMessage<String, String> keyedMessage = new KeyedMessage<>("dev-1.0_mp-catalog.RT","key"+1,content);
             producer.send(keyedMessage);
             System.out.println("Message sent successfully on DEV cluster...");
         }else{
        	 System.out.println("Something went wrong while reading file...");
         }
         
        
	}	
	
	public static String readFileByAllBytes(String filePath) {
		String content = null;
		try {
			content = new String(Files.readAllBytes(Paths.get(filePath)));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return content;
	}
}