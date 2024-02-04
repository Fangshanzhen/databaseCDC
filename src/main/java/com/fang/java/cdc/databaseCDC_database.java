package com.fang.java.cdc;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;
import org.pentaho.di.core.row.RowMetaInterface;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.fang.java.CDCUtils.*;


/**
 * 各关系型数据库通用cdc，写进中间库的版本
 */


@Slf4j
public class databaseCDC_database {

    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("数据CDC增量-写进中间库");

    public static void cdcData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                               String originalUsername, String originalPassword,
                               String tableList, String offsetAddress, String databaseHistoryAddress, String serverId,
                               String targetDatabaseType, String targetDbname, String targetSchema, String targetIp, String targetPort,
                               String targetUsername, String targetPassword, String index, //索引字段名 ipid,pid 复合索引以逗号隔开
                               String indexName,
                               String etlTime

    ) throws Exception {


        KettleEnvironment.init();
        DatabaseMeta originalDbmeta = null;
        DatabaseMeta targetDbmeta = null;
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
                    String modified = transformString(tableList, originalSchema);

                    if (allTableList.size() > 0) {
                        for (String table : allTableList) {
                            String sql = null;
                            if (originalDatabaseType.equals("ORACLE")) {
                                sql = "select * from " + originalSchema + "." + table + " where rownum <=10 ";  //用sql来获取字段名及属性以便在目标库中创建表
                            } else if (originalDatabaseType.equals("MSSQL")) {
                                sql = "select top 10 * from " + originalDatabase + "." + originalSchema + "." + table;   //sqlserver  没有limit 用top
                            } else {
                                sql = "select * from " + originalSchema + "." + table + "  limit 10;";
                            }

                            RowMetaInterface rowMetaInterface = originalDatabase.getQueryFieldsFromPreparedStatement(sql);

                            String sql1 = targetDatabase.getDDLCreationTable(targetSchema + "." + table, rowMetaInterface);

                            if (etlTime != null && etlTime.length() > 0) {
                                int a = sql1.lastIndexOf(")"); //最后一个)
                                if (a > 0) {
                                    sql1 = sql1.replace(sql1.substring(a), "");
                                }
                                sql1 = sql1 + ",";
                                sql1 = sql1 + etlTime + "  " + "TIMESTAMP " + " NOT NULL DEFAULT CURRENT_TIMESTAMP";
                                sql1 = sql1 + Const.CR + ");";

                            }

                            if (sql1.length() > 0) {
                                if (!checkTableExist(targetDatabase, targetSchema, table)) {  //判断目标数据库中表是否存在
                                    //建索引、创建表
                                    sameCreate(table, sql1, rowMetaInterface, originalDbmeta, originalDatabaseType, targetDatabaseType, targetSchema, targetDatabase, index, indexName); //创建表
                                    kettleLog.logBasic(table + " 创建输出表成功！");
                                }
                            }

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

                            if (originalDatabaseType.toLowerCase().equals("postgresql")) {
                                config = config.edit().with("slot.name", "debezium1234") // postgresql 单独配置， max_replication_slots = 20
                                        .with("plugin.name", "pgoutput").build();      //postgresql 单独配置，必须是这个名字
                            }
                            if (originalDatabaseType.toLowerCase().equals("mysql")) {
                                config = config.edit()
                                        .with("database.server.id", serverId)   //填上mysql的 serverid
                                        .build();      //
                            }

                            EmbeddedEngine engine = EmbeddedEngine.create()
                                    .using(config)
                                    .notifying(record -> {
                                        Struct structValue = (Struct) record.value();
                                        try {
                                            commonCrud(structValue, table, targetSchema, targetDatabaseType, targetIp, targetPort, targetUsername, targetPassword, targetDbname, etlTime, index);
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
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




