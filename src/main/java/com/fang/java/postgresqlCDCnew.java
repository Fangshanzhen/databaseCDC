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
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


@Slf4j
public class postgresqlCDCnew {


    public static void incrementData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                                     String originalUsername, String originalPassword,
                                     String tableList, String kafkaipport, String topic,String offsetAddress,String databaseHistoryAddress) throws Exception {


        if (tableList != null) {//填入表名的
            String modified = transformString(tableList, originalSchema);

            List<String> fileList = new ArrayList<>();
            fileList.add(offsetAddress);
            fileList.add(databaseHistoryAddress);
            try {
                for (String s : fileList) {
                    File file = new File(s);
                    if (file.createNewFile()) {
                        log.info("File created: " + file.getName());
                    } else {
                        log.info("File already exists.");
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
                    .with("database.server.name", "my-postgresql-server")
                    .with("table.include.list", modified)
                    .with("include.schema.changes", "false")
                    .with("name", "my-connector-postgresql-0")
                    .with("offset.storage", FileOffsetBackingStore.class.getName()) //选择FileOffsetBackingStore时,意思把读取进度存到本地文件，当使用kafka时，选KafkaOffsetBackingStore
                    .with("offset.storage.file.filename", offsetAddress)//存放读取进度的本地文件地址。
                    .with("offset.flush.interval.ms", 2000)  //设置得合理，不然会导致记录得偏移量还未来得及写进文件中
                    .with("database.history", FileDatabaseHistory.class.getName())
                    .with("database.history.file.filename", databaseHistoryAddress)//Debezium用来记录数据库架构变化历史的本地文件路径。数据库进行架构更改（如添加、删除表或字段等）时，Debezium需要跟踪这些更改来正确解析和转换数据变更事件
                    .with("logger.level", "DEBUG")
                    .with("slot.name", "debezium123") // postgresql 单独配置，注意不可重复  max_replication_slots = 20
                    .with("plugin.name", "pgoutput")        //postgresql 单独配置,必须是这个名字
                    .with("snapshot.mode", "initial")

                    /**
                     * 1. `initial`: 它在Debezium首次启动时将对整个数据库表进行一次完整的读取，以获取数据库最初的状态。
                     * 2. `schema_only`: 在这个模式下，Debezium仅仅捕获数据库表结构的快照，而不会捕获每一行数据的内容。
                     * 3. `never`: 在这个模式下，Debezium不会对数据库表进行任何初始快照，仅捕获启动后发生的数据变更。
                     * 4. `initial_only`: 与`initial`模式相似，它会在启动时对数据库进行完整快照，但是捕获完快照后Debezium将停止运行。
                     */



                    .build();

//            EmbeddedEngine engine = EmbeddedEngine.create()
//                    .using(config)
//                    .notifying(record -> {
//                        System.out.println("---------------" + record);
//                        String key = String.valueOf(record.key());
//
//                        Struct structValue = (Struct) record.value();
//                        JSONObject jsonObject = transformData(structValue, key);
//
//
//                    })
//                    .build();
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            EmbeddedEngine engine = EmbeddedEngine.create()
                    .using(config)
                    .notifying(record -> {
                        // 使用ExecutorService来异步地处理记录并获取JSONObject
                        Future<JSONObject> futureJsonObject = executorService.submit(() -> {
                            Struct structValue = (Struct) record.value();
                            return CDCUtils.transformData(structValue);
                        });

                        // 等待Future任务完成并获取结果
                        try {
                            JSONObject jsonObject = futureJsonObject.get();
                            System.out.println(jsonObject);
                            //todo 处理JSONObject

                            try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {
                                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, tableList, String.valueOf(jsonObject));
                                kafkaProducer.send(producerRecord).get();
                                log.info("数据写入kafka成功！");
                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    })
                    .build();

            // 启动 engine
            engine.run();

        }
    }


    public static String transformString(String original, String prefix) {
        // 分割原始字符串
        String[] elements = original.split(",");
        StringBuilder result = new StringBuilder();

        // 遍历所有元素，给每个元素加上前缀
        for (int i = 0; i < elements.length; i++) {
            result.append(prefix).append("."); // 加上前缀
            result.append(elements[i]); // 加上原始元素
            if (i < elements.length - 1) {
                result.append(","); // 如果不是最后一个元素，则加上逗号
            }
        }

        return result.toString();
    }


}




