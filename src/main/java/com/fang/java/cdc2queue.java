package com.fang.java;

import com.alibaba.fastjson.JSONObject;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.fang.java.CDCUtils.*;


@Slf4j
public class cdc2queue {
    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("监听数据");

    public static void cdcData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                               String originalUsername, String originalPassword,
                               String tableList, String offsetAddress, String databaseHistoryAddress, String serverId, BlockingQueue queue) throws Exception {

        if (tableList != null) {
            String modified = transformString(tableList, originalSchema);
            //创建存放目录
            createFile(offsetAddress, databaseHistoryAddress);

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
//                String slotName = getSlotName(originalIp, originalPort, originalSchema, originalUsername, originalPassword, originalDbname);
                config = config.edit().with("slot.name", "debezium_slot") // postgresql 单独配置， 逻辑复制槽名称, 不能超过max_replication_slots = 20
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
                        // 将转换后的JSON对象放入队列，等待被下一个节点消费
//                        queue.offer(operateJson);
                        try {
                            queue.put(operateJson);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        kettleLog.logBasic("监听数据：" + operateJson);
                    })
                    .build();

            // 启动一个线程来运行Debezium Engine
            new Thread(() -> {
                engine.run();
            }).start();

            // 启动OperateJsonProcessor来处理队列中的数据
//            OperateJsonProcessor processor = new OperateJsonProcessor(queue);
//            new Thread(processor).start();
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




