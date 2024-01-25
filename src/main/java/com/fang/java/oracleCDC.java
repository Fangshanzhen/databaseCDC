package com.fang.java;


import lombok.extern.slf4j.Slf4j;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import com.alibaba.fastjson.JSONObject;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;

import java.io.File;
import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.fang.java.CDCUtils.*;

/**
 * 本地oracle    debezium 已测试成功，数据有延迟约1分钟左右
 */
@Slf4j
public class oracleCDC {


    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("ORACLE数据库CDC增量");

    public static void incrementData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                                     String originalUsername, String originalPassword,
                                     String targetDatabaseType, String targetDbname, String targetSchema, String targetIp, String targetPort,
                                     String targetUsername, String targetPassword,
                                     String tableList, //表名，多表以逗号隔开，//
                                     String kafkaipport,
                                     String topic,
                                     String index, //索引字段名 ipid,pid 复合索引以逗号隔开
                                     String indexName,
                                     String etlTime //索引名称
    ) throws Exception {


        KettleEnvironment.init();
        DatabaseMeta originalDbmeta = null; //
        DatabaseMeta targetDbmeta = null; //
        try {

            originalDbmeta = new DatabaseMeta(originalDbname, originalDatabaseType, "Native(JDBC)", originalIp, originalDbname, originalPort, originalUsername, originalPassword);
            targetDbmeta = new DatabaseMeta(targetDbname, targetDatabaseType, "Native(JDBC)", targetIp, targetDbname, targetPort, targetUsername, targetPassword);
        } catch (Exception e) {
            kettleLog.logError(e + "");
        }
        kettleLog.logBasic("源数据库、目标数据库连接成功！");
        if (originalDbmeta != null && targetDbmeta != null) {
            Database originalDatabase = new Database(originalDbmeta);
            originalDatabase.connect();
            Database targetDatabase = new Database(targetDbmeta);
            targetDatabase.connect(); //连接数据库

            try {
                if (tableList != null) {//填入表名的
                    List<String> allTableList = null;
                    if (tableList.contains(",")) {
                        allTableList = Arrays.asList(tableList.split(","));
                    } else {
                        allTableList = Collections.singletonList(tableList);
                    }


                    if (allTableList.size() > 0) {
                        for (String table : allTableList) {
                            String sql = null;

                            createTable(sql,originalDatabaseType,originalDbmeta,originalSchema,originalDatabase,table,targetDatabase,targetSchema,targetDatabaseType, etlTime,index,indexName);

                            // Kafka properties
                            final Properties kafkaProperties = new Properties();
                            kafkaProperties.setProperty("bootstrap.servers", kafkaipport);
                            kafkaProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                            kafkaProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                            kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test0719");
//        kafkaProperties.setProperty("auto.offset.reset", "earliest");

                            // Debezium connector configuration

                            List<String> fileList = new ArrayList<>();
                            fileList.add("D:\\Debezium\\offset\\oracle\\dbhistory.dat");
                            fileList.add("D:\\Debezium\\offset\\oracle\\file.dat");
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

                            Configuration config = Configuration.create()
                                    .with("name", "my-connector-4")
                                    .with("offset.storage", FileOffsetBackingStore.class.getName())
                                    .with("offset.storage.file.filename", "D:\\\\Debezium\\\\offset\\\\oracle\\\\file.dat")
//                .with("offset.storage", MemoryOffsetBackingStore.class.getName())
//                .with("offset.storage.file.filename", "/path/to/offset/file.dat")
                                    .with("offset.flush.interval.ms", 2000)
                                    .with("database.history", FileDatabaseHistory.class.getName())
                                    .with("database.history.file.filename", "D:\\\\Debezium\\\\offset\\\\oracle\\\\dbhistory.dat")
                                    .with("connector.class", "io.debezium.connector.oracle.OracleConnector")
                                    .with("database.hostname", originalIp)
                                    .with("database.port", originalPort)
                                    .with("database.user", originalUsername)
                                    .with("database.password", originalPassword)
                                    .with("database.dbname", originalDbname)
                                    .with("database.server.name", "my-oracle-server2")
                                    .with("schema.whitelist", originalSchema)
                                    .with("table.include.list", originalSchema + "." + table)

//                .with("snapshot.mode", "initial")
                                    .build();


                            EmbeddedEngine engine = EmbeddedEngine.create()
                                    .using(config)
                                    .notifying(record -> {
                                        String key = String.valueOf(record.key());
                                        Struct structValue = (Struct) record.value();

                                        commonCrud(structValue, table,key, topic,  kafkaProperties, targetSchema, targetDatabaseType, targetIp, targetPort, targetUsername, targetPassword, targetDbname, etlTime,index);

                                    })
                                    .build();
                            engine.run();

                        }

                    }
                }
            } finally {
                originalDatabase.disconnect();
                targetDatabase.disconnect();
            }
        }
    }



}







