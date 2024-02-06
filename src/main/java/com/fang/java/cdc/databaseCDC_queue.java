package com.fang.java.cdc;

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

/**
 * 各关系型数据库通用cdc，写进队列的版本
 * 参考文档：https://debezium.io/documentation/reference/nightly/connectors/
 */
@Slf4j
public class databaseCDC_queue {
    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("监听数据");

    public static void cdcData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                               String originalUsername, String originalPassword,
                               String tableList, String offsetAddress, String databaseHistoryAddress, String serverId, BlockingQueue queue, String slotName) throws Exception {

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
//                    .with("include.schema.changes", "false")  //表结构方面的
                    .with("name", "my-connector-" + originalDatabaseType)
                    .with("offset.storage", FileOffsetBackingStore.class.getName())
                    .with("offset.storage.file.filename", offsetAddress)
                    .with("offset.flush.interval.ms", 2000)
                    .with("database.history", FileDatabaseHistory.class.getName())
                    .with("database.history.file.filename", databaseHistoryAddress)
                    .with("logger.level", "INFO")
                    .with("snapshot.mode", "initial") //首次全量
                    .with("database.serverTimezone", "Asia/Shanghai")

                    .build();

            if (originalDatabaseType.equals("postgresql")) {
//                String slotName = getSlotName(originalIp, originalPort, originalSchema, originalUsername, originalPassword, originalDbname);
                config = config.edit().with("slot.name", slotName) // postgresql 单独配置， 逻辑复制槽名称, 不能超过max_replication_slots = 20
                        .with("plugin.name", "pgoutput").build();      //postgresql 单独配置，必须是这个名字
            }
            if (originalDatabaseType.equals("mysql")) {
                config = config.edit()
                        .with("database.server.id", serverId)   //填上mysql的 serverid
                        .with("converters", "dateConverters")   //解决mysql字段中的时区问题，设置with("database.serverTimezone", "Asia/Shanghai")无效
                        .with("dateConverters.type", "com.fang.java.cdc.MySqlDateTimeConverter")
                        .build();      //
            }


            EmbeddedEngine engine = EmbeddedEngine.create()
                    .using(config)
                    .notifying(record -> {
                        if (queue == null) {
                            kettleLog.logBasic("cdc监听数据暂停.....");
                            return;
                        }
                        Struct structValue = (Struct) record.value();
                        JSONObject operateJson = transformData(structValue, originalDatabaseType);
                        // 将转换后的JSON对象放入队列，等待被下一个节点消费
                        if (operateJson != null && operateJson.keySet().size() > 0) {
                            queue.offer(operateJson);
                            kettleLog.logBasic("cdc数据写进队列：" + operateJson);
                        }
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


}




