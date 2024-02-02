package com.fang.java;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;
import org.pentaho.di.core.row.RowMetaInterface;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.fang.java.CDCUtils.*;

@Slf4j
public class postgresqlCDCnew {

    /**
     * postgresql 本次测试成功
     * https://my.oschina.net/u/3425541/blog/5534458

     */

    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("POSTGRESQL数据库CDC增量");

    public static void incrementData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                                     String originalUsername, String originalPassword,
                                     String tableList, //表名，多表以逗号隔开，//
                                     String kafkaipport,
                                     String topic, String a, String b
    ) throws Exception {


        if (tableList != null) {//填入表名的
            List<String> allTableList = null;
            if (tableList.contains(",")) {
                allTableList = Arrays.asList(tableList.split(","));
            } else {
                allTableList = Collections.singletonList(tableList);
            }


            if (allTableList.size() > 0) {
                for (String table : allTableList) {


                    Properties props = new Properties();
                    props.put("bootstrap.servers", kafkaipport);
                    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

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


                    Configuration config = Configuration.create()
                            .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                            .with("database.hostname", originalIp)
                            .with("database.port", originalPort)
                            .with("database.user", originalUsername)
                            .with("database.password", originalPassword)
                            .with("database.dbname", originalDbname)
                            .with("database.server.name", "my-postgresql-server2")
                            .with("table.include.list", originalSchema + "." + table)
                            .with("include.schema.changes", "false")
                            .with("name", "my-connector-postgresql-0")
                            .with("offset.storage", FileOffsetBackingStore.class.getName())
                            .with("offset.storage.file.filename", "D:\\\\Debezium\\\\offset\\\\postgresql\\\\file.dat")
                            .with("offset.flush.interval.ms", 2000)  //这个时间得设置得合理，不然会导致记录得偏移量还未来得及写进文件中
                            .with("database.history", FileDatabaseHistory.class.getName())
                            .with("database.history.file.filename", "D:\\\\Debezium\\\\offset\\\\postgresql\\\\dbhistory.dat")
                            .with("logger.level", "DEBUG")
                            .with("slot.name", "debezium123") // postgresql 单独配置，注意不可重复
                            .with("plugin.name", "pgoutput")        //postgresql 单独配置

                            .build();

                    EmbeddedEngine engine = EmbeddedEngine.create()
                            .using(config)
                            .notifying(record -> {
                                System.out.println("---------------" + record);
                                String key = String.valueOf(record.key());

                                Struct structValue = (Struct) record.value();
                                JSONObject operateJson = transformData(structValue, originalDatabaseType);
                                KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
                                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, String.valueOf(operateJson));
                                try {
                                    kafkaProducer.send(producerRecord).get();
                                } catch (InterruptedException | ExecutionException e) {
                                    e.printStackTrace();
                                } finally {
                                    kafkaProducer.close();
                                }
                                kettleLog.logBasic("数据写入kafka成功！");
                            })
                            .build();

                    // 启动 engine
                    engine.run();

                }

            }
        }


    }

}
