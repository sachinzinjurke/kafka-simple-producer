package com.sears.kafka;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaSimpleProducerOldImpl{

	 //String filePath = "C://workspaces//kafka-tutorial//kafka-simple-producer//src//main//resources//input.txt";
	
	public static void main(String[] args) {
		 String filePath = "C://workspaces//kafka-tutorial//kafka-simple-producer//src//main//resources//MCR.txt";
		Properties props = new Properties();

        props.put("metadata.broker.list", "localhost:9092");
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
        	 KeyedMessage<String, String> keyedMessage = new KeyedMessage<>("qa-2.0_mpcatalog.RT","key"+1,content);
             producer.send(keyedMessage);
             System.out.println("Message sent successfully...");
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