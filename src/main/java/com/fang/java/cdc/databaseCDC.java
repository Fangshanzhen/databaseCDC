package com.fang.java.cdc;

import com.alibaba.fastjson.JSONObject;
import com.fang.java.CDCUtils;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.fang.java.CDCUtils.*;


/**
 * 各关系型数据库通用cdc，写进kafka的版本
 */


@Slf4j
public class databaseCDC {

    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("数据CDC增量");

    public static void cdcData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                               String originalUsername, String originalPassword,
                               String tableList, String kafkaServer, String topic, String offsetAddress, String databaseHistoryAddress, String serverId) throws Exception {

        KettleEnvironment.init();
        if (tableList != null) {
            String modified = transformString(tableList, originalSchema);
            //创建存放目录
            createFile(offsetAddress, databaseHistoryAddress);

            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaServer);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


            Properties properties = new Properties();
            properties.setProperty("converters","dateConverters");
            properties.setProperty("dateConverters.type","com.fang.java.cdc.MySqlDateTimeConverter");


            Configuration config = Configuration.create()
                    .with("connector.class", connectorClass(originalDatabaseType))
                    .with("database.hostname", originalIp)
                    .with("database.port", originalPort)
                    .with("database.user", originalUsername)
                    .with("database.password", originalPassword)
                    .with("database.dbname", originalDbname)
                    .with("database.server.name", "my-cdc-server-" + originalDatabaseType)
                    .with("table.include.list", modified)
                    .with("database.schema", originalSchema)
                    .with("include.schema.changes", "false")
                    .with("name", "my-connector-" + originalDatabaseType)
                    .with("offset.storage", FileOffsetBackingStore.class.getName())
                    .with("offset.storage.file.filename", offsetAddress)
                    .with("offset.flush.interval.ms", 2000)
                    .with("database.history", FileDatabaseHistory.class.getName())
                    .with("database.history.file.filename", databaseHistoryAddress)
                    .with("logger.level", "DEBUG")
                    .with("snapshot.mode", "initial") //首次全量
                    .with("database.serverTimezone", "Asia/Shanghai")

                    .build();

            if (originalDatabaseType.equals("postgresql")) {
                config = config.edit().with("slot.name", "debezium12") // postgresql 单独配置， max_replication_slots = 20
                        .with("plugin.name", "pgoutput").build();      //postgresql 单独配置，必须是这个名字
            }
            if (originalDatabaseType.equals("mysql")) {
                config = config.edit()
                        // .with("database.port", Integer.valueOf(originalPort)) //mysql port为数值型
                        .with("database.server.id", serverId)   //填上mysql的 serverid
                        .with("converters", "dateConverters")   //解决mysql字段中的时区问题，设置with("database.serverTimezone", "Asia/Shanghai")无效
                        .with("dateConverters.type", "com.fang.java.cdc.MySqlDateTimeConverter")
                        .build();      //
            }

            ExecutorService executorService = Executors.newSingleThreadExecutor();
            EmbeddedEngine engine = EmbeddedEngine.create()
                    .using(config)
                    .notifying(record -> {
                        // 使用ExecutorService来异步处理记录并获取JSONObject
                        Future<JSONObject> futureJsonObject = executorService.submit(() -> {
                            Struct structValue = (Struct) record.value();
                            kettleLog.logBasic("structValue:   " + structValue);
                            return CDCUtils.transformData(structValue, originalDatabaseType);
                        });

                        // 等待Future任务完成并获取结果
                        try {
                            JSONObject jsonObject = futureJsonObject.get();

                            //todo 处理JSONObject
                            if (jsonObject != null && jsonObject.keySet().size() > 0) {
                                try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {
                                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, tableList, String.valueOf(jsonObject));
                                    kafkaProducer.send(producerRecord).get();
                                    kettleLog.logBasic("数据写入kafka成功！");
                                } catch (InterruptedException | ExecutionException e) {
                                    kettleLog.logError("" + e);
                                }
                            }

                        } catch (Exception e) {
                            kettleLog.logError("" + e);
                        }
                    })
                    .build();

            // 启动 engine
            engine.run();

        }
    }




}




