package com.fang.java;

import org.json.JSONObject;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;

import java.time.Duration;
import java.util.*;

@Slf4j
public class kafkaToKettle {
    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("消费kafka");

    public static void createTransMeta(String ip, String topic) throws Exception {

        kettleResponse kettleResponse = new kettleResponse();

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

                Iterator<String> keys = sqlJsonObj.keys();

                RowMetaInterface outputRowMeta = new RowMeta();
                Map<String, Object> outputRowDataMap = new HashMap<>();

                while (keys.hasNext()) {
                    String key = keys.next();
                    Object value = sqlJsonObj.get(key);
                    ValueMetaInterface valueMeta = new ValueMetaString(key);
                    outputRowMeta.addValueMeta(valueMeta);
                    outputRowDataMap.put(key, value);
                }

                // Create output row
                Object[] outputRowData = new Object[outputRowMeta.size()];
                for (int i = 0; i < outputRowMeta.size(); i++) {
                    ValueMetaInterface valueMeta = outputRowMeta.getValueMeta(i);
                    outputRowData[i] = outputRowDataMap.get(valueMeta.getName());
                }
                kettleResponse.setOutputRowData(outputRowData);
                kettleResponse.setOutputRowMeta(outputRowMeta);

            }


        }

    }
}