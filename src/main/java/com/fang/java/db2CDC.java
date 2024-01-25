package com.fang.java;

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

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.fang.java.CDCUtils.*;

@Slf4j
public class db2CDC {

    /**

     */

    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("DB2数据库CDC增量");

    public static void incrementData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                                     String originalUsername, String originalPassword,
                                     String targetDatabaseType, String targetDbname, String targetSchema, String targetIp, String targetPort,
                                     String targetUsername, String targetPassword,
                                     String tableList, //表名，多表以逗号隔开，//
                                     String kafkaipport,
                                     String topic,
                                     String index, //索引字段名 ipid,pid 复合索引以逗号隔开
                                     String indexName,
                                     String etlTime) throws Exception //索引名称
    {


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
                            if (originalDatabaseType.equals("ORACLE")) {
                                sql = "select * from " + originalSchema + "." + table + " where rownum <=10 ";  //用sql来获取字段名及属性以便在目标库中创建表
                            } else if (originalDatabaseType.equals("MSSQL")) {
                                sql = "select top 10 * from " + originalDatabase + "." + originalSchema + "." + table;   //sqlserver  没有limit 用top
                            } else {
                                sql = "select * from " + originalSchema + "." + table + "  limit 10;";
                            }

                            RowMetaInterface rowMetaInterface = originalDatabase.getQueryFieldsFromPreparedStatement(sql);

                            String sql1 = targetDatabase.getDDLCreationTable(targetSchema + "." + table, rowMetaInterface);

                            if (etlTime.length() > 0) {
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


                            Properties props = new Properties();
                            props.put("bootstrap.servers", kafkaipport);
                            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                            List<String> fileList = new ArrayList<>();
                            fileList.add("D:\\Debezium\\offset\\db2\\file.dat");
                            fileList.add("D:\\Debezium\\offset\\db2\\dbhistory.dat");
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
                                    .with("connector.class", "io.debezium.connector.db2.Db2Connector")
                                    .with("database.hostname", originalIp)
                                    .with("database.port", originalPort)
                                    .with("database.user", originalUsername)
                                    .with("database.password", originalPassword)
                                    .with("database.dbname", originalDbname)
                                    .with("database.server.name", "my-db2-server1")
                                    .with("table.include.list", originalSchema + "." + table)
                                    .with("include.schema.changes", "false")
                                    .with("name", "my-connector-db2-1")
                                    .with("offset.storage", FileOffsetBackingStore.class.getName())
                                    .with("offset.storage.file.filename", "D:\\\\Debezium\\\\offset\\\\db2\\\\file.dat")
                                    .with("offset.flush.interval.ms", 6000)
                                    .with("database.history", FileDatabaseHistory.class.getName())
                                    .with("database.history.file.filename", "D:\\\\Debezium\\\\offset\\\\db2\\\\dbhistory.dat")

                                    .with("logger.level", "DEBUG")

                                    .build();

                            EmbeddedEngine engine = EmbeddedEngine.create()
                                    .using(config)
                                    .notifying(record -> {
                                        String key = String.valueOf(record.key());

                                        Struct structValue = (Struct) record.value();

                                        commonCrud(structValue, table,key, topic,  props, targetSchema, targetDatabaseType, targetIp, targetPort, targetUsername, targetPassword, targetDbname, etlTime,index);

                                    })
                                    .build();

                            // 启动 engine
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
