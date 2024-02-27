package com.fang.java.test;


import io.debezium.config.Configuration;

import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.fang.java.mongodbUtils.structToJson;

/**
 * 测试成功 1.6.4.Final测试成功，1.9.7版本失败
 */
public class mongoTest {


    public static void main(String[] args) {


        List<String> fileList = new ArrayList<>();
        fileList.add("D:\\Debezium\\offset\\mongodb\\dbhistory.dat");
        fileList.add("D:\\Debezium\\offset\\mongodb\\file.dat");
        try {
            for (String s : fileList) {
                File file = new File(s);
                if (file.createNewFile()) {
                    System.out.println("File created: " + file.getName());
                } else {
                    System.out.println("File already exists.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.108.51:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        // 配置Debezium连接MongoDB的相关参数
        Configuration config = Configuration
                .create()
                .with("name", "my-mongodb-1")
                .with("connector.class", "io.debezium.connector.mongodb.MongoDbConnector")
                .with("mongodb.hosts", "aiit-zhyl/10.0.108.31:27017")
                .with("mongodb.name", "test")
                .with("database.whitelist", "test")
                .with("collection.whitelist", "test.test")
                .with("offset.storage", FileOffsetBackingStore.class.getName())
                .with("offset.storage.file.filename", "D:\\\\Debezium\\\\offset\\\\mongodb\\\\file.dat")
                .with("offset.flush.interval.ms", 1000)
                .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
                .with("database.history.file.filename", "D:\\\\Debezium\\\\offset\\\\mongodb\\\\dbhistory.dat")
                .with("snapshot.mode", "initial")
                .build();

        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .notifying(record -> {

                    Struct structValue = (Struct) record.value();
                    System.out.println("--------------------" + structValue);
                    //Struct{after={"_id": {"$oid": "64c9ec2b8d5e7cc94c519cf7"},"name": "John","age": 25.0,"email": "john@example.com"},source=Struct{version=1.6.4.Final,connector=mongodb,name=test,ts_ms=1706164237000,snapshot=true,db=test,rs=aiit-zhyl,collection=test,ord=1},op=r,ts_ms=1706164239764}
                    //Struct{after={"_id": {"$oid": "65b1fd39b6a2b55bdb9d49ed"},"name": "66666","age": 666666.0,"email": "100023@example.com"},source=Struct{version=1.6.4.Final,connector=mongodb,name=test,ts_ms=1706163514000,db=test,rs=aiit-zhyl,collection=test,ord=1},op=c,ts_ms=1706163513674}
                    //Struct{patch={"$v": 1,"$set": {"age": 666666.0}},filter={"_id": {"$oid": "65b1fba8b6a2b55bdb9d49ec"}},source=Struct{version=1.6.4.Final,connector=mongodb,name=test,ts_ms=1706163473000,db=test,rs=aiit-zhyl,collection=test,ord=1},op=u,ts_ms=1706163472767}
                    JSONObject jsonObject = structToJson(structValue);
                    if (jsonObject.keySet().size() > 0) {
                        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {
                            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("mongodb", "test", String.valueOf(jsonObject));
                            kafkaProducer.send(producerRecord).get();
                        } catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                    }

                })
                .build();

        // 启动 engine
        engine.run();
    }


}
