package com.fang.java.test;

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

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.fang.java.CDCUtils.*;


/**
 * 测试用
 */


@Slf4j
public class test0129 {

    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("数据CDC增量");

    public static void cdcData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                               String originalUsername, String originalPassword,
                               String tableList, String kafkaServer, String topic, String offsetAddress, String databaseHistoryAddress, String serverId) throws Exception {


        if (tableList != null) {
            String modified = transformString(tableList, originalSchema);
            //创建存放目录
            createFile(offsetAddress, databaseHistoryAddress);

            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaServer);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

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
                        .with("database.server.id", serverId)   //填上mysql的 serverid
                        .build();      //
            }

            EmbeddedEngine engine = EmbeddedEngine.create()
                    .using(config)
                    .notifying(record -> {
                        Struct structValue = (Struct) record.value();
                        JSONObject operateJson = transformData(structValue, originalDatabaseType);
                        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, tableList, String.valueOf(operateJson));
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

            engine.run();

        }
    }


    private static String connectorClass(String originalDatabaseType) {
        if (originalDatabaseType.equals("postgresql")) {
            return "io.debezium.connector.postgresql.PostgresConnector";
        }
        if (originalDatabaseType.equals("mysql")) {
            return "io.debezium.connector.mysql.MySqlConnector";
        }
        if (originalDatabaseType.equals("oracle")) {
            return "io.debezium.connector.oracle.OracleConnector";
        }
        if (originalDatabaseType.equals("sqlserver")) {
            return "io.debezium.connector.sqlserver.SqlServerConnector";
        }
        if (originalDatabaseType.equals("db2")) {
            return "io.debezium.connector.db2.Db2Connector";
        }

        return null;
    }


}




