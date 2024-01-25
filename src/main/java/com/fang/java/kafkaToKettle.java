package com.fang.java;

import org.json.JSONObject;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.time.Duration;
import java.util.*;

@Slf4j
public class kafkaToKettle {

    public static void createTransMeta(String ip, String topic) throws Exception {


        Properties props = new Properties();
        props.put("bootstrap.servers", ip);
        props.put("group.id", "test-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        // 创建Kafka消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅主题
        consumer.subscribe(Collections.singletonList(topic));


        while (true) {
            // 获取数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                // 提取记录的JSON内容
                String json = record.value();
                JSONObject jsonObj = new JSONObject(json);
                // 处理JSON数据
                JSONObject sqlJsonObj = jsonObj.getJSONObject("sqlJson");


            }


        }

    }
}