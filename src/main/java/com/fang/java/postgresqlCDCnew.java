package com.fang.java;

import com.alibaba.fastjson.JSONObject;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.fang.java.CDCUtils.*;


@Slf4j
public class postgresqlCDCnew {


    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("postgresql数据库CDC增量");

    public static void incrementData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                                     String originalUsername, String originalPassword,
                                     String tableList, String kafkaipport, String topic) throws Exception {


        if (tableList != null) {//填入表名的
            String modified = transformString(tableList, originalSchema);


//
            List<String> fileList = new ArrayList<>();
            fileList.add("D:\\Debezium\\offset\\postgresql\\file.dat");
            fileList.add("D:\\Debezium\\offset\\postgresql\\dbhistory.dat");
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
            props.put("bootstrap.servers", kafkaipport);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


            Configuration config = Configuration.create()
                    .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                    .with("database.hostname", originalIp)
                    .with("database.port", originalPort)
                    .with("database.user", originalUsername)
                    .with("database.password", originalPassword)
                    .with("database.dbname", originalDbname)
                    .with("database.server.name", "my-postgresql-server2")
                    .with("table.include.list", modified)
                    .with("include.schema.changes", "false")
                    .with("name", "my-connector-postgresql-0")
                    .with("offset.storage", FileOffsetBackingStore.class.getName())
                    .with("offset.storage.file.filename", "D:\\\\Debezium\\\\offset\\\\postgresql\\\\file.dat")
                    .with("offset.flush.interval.ms", 2000)  //这个时间得设置得合理，不然会导致记录得偏移量还未来得及写进文件中，目前设定2秒钟
                    .with("database.history", FileDatabaseHistory.class.getName())
                    .with("database.history.file.filename", "D:\\\\Debezium\\\\offset\\\\postgresql\\\\dbhistory.dat")
                    .with("logger.level", "DEBUG")
                    .with("slot.name", "debezium12") // postgresql 单独配置，注意不可重复
                    .with("plugin.name", "pgoutput")        //postgresql 单独配置,必须是这个名字

                    .build();
            ExecutorService executorService = Executors.newSingleThreadExecutor();
//            EmbeddedEngine engine = EmbeddedEngine.create()
//                    .using(config)
//                    .notifying(record -> {
//                        System.out.println("---------------" + record);
//                        String key = String.valueOf(record.key());
//
//                        Struct structValue = (Struct) record.value();
//                        JSONObject jsonObject = kettleData(structValue, key);
//
//
//                    })
//                    .build();

            EmbeddedEngine engine = EmbeddedEngine.create()
                    .using(config)
                    .notifying(record -> {
                        // 使用ExecutorService来异步地处理记录并获取JSONObject
                        Future<JSONObject> futureJsonObject = executorService.submit(() -> {
                            String key = String.valueOf(record.key());
                            Struct structValue = (Struct) record.value();
                            return kettleData(structValue, key);
                        });

                        // 等待Future任务完成并获取结果
                        try {
                            JSONObject jsonObject = futureJsonObject.get();
                            System.out.println(jsonObject);
                            //todo 在这里处理JSONObject

                            try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {
                                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, tableList, String.valueOf(jsonObject));
                                kafkaProducer.send(producerRecord).get();

                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }
                            kettleLog.logBasic("数据写入kafka成功！");

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    })
                    .build();

            // 启动 engine
            engine.run();


        }

    }


}




