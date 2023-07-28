package com.fang.java.test;



import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 中心医院oracle不能配置    debezium
 */
public class test10 {


    public static void main(String[] args)  {


        String kafkaBootstrapServers = "10.0.108.51:9092";

        String user = "huayin";
        String password = "huayin";

        // Kafka properties
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", kafkaBootstrapServers);
        kafkaProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test3");
//        kafkaProperties.setProperty("auto.offset.reset", "earliest");

        // Debezium connector configuration
        Configuration config = Configuration.create()
                .with("name", "my-connector-3")
                .with("offset.storage", FileOffsetBackingStore.class.getName())
                .with("offset.storage.file.filename", "/path/to/offset/file.dat")
//                .with("offset.storage", MemoryOffsetBackingStore.class.getName())
//                .with("offset.storage.file.filename", "/path/to/offset/file.dat")
                .with("offset.flush.interval.ms", 60000)
                .with("database.history", FileDatabaseHistory.class.getName())
                .with("database.history.file.filename", "/path/to/dbhistory.dat")
                .with("connector.class", "io.debezium.connector.oracle.OracleConnector")
                .with("database.hostname", "172.16.202.120")
                .with("database.port", "1521")
                .with("database.user", "huayin")
                .with("database.password", "huayin")
                .with("database.dbname", "ORCL")
                .with("database.server.name", "my-oracle-server1")
                .with("schema.whitelist", "HUAYIN")
                .with("table.include.list", "HUAYIN.TEST")
                .with("database.history.kafka.bootstrap.servers", "10.0.108.51:9092")
                .with("database.history.kafka.topic", "debezium_oracle")
                .build();


        // Create and start the Debezium embedded engine
        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .notifying(record -> {
                    String topic = "test3";
                    String key = String.valueOf(record.key());
                    String value = String.valueOf(record.value());
                    System.out.printf("Received message from topic: %s, key: %s, value: %s%n", topic, key, value);

                    // Send the Debezium event to Kafka
                    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties);
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                    try {
                        RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                        System.out.printf("Sent message to topic: %s, partition: %s, offset: %s%n",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    } finally {
                        kafkaProducer.close();
                    }
                })
                .build();

        // Start the Debezium engine
        engine.run();

        // Create a Kafka consumer to consume events from the topic
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test3");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("test3"));

        // Consume and process events from the topic
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                System.out.printf("Consumed message from topic: %s, partition: %s, offset: %s, key: %s, value: %s%n",
                        record.topic(), record.partition(), record.offset(), key, value);
                // Process the consumed message here
            }
        }

    }




}







