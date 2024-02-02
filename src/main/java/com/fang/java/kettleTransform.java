package com.fang.java;

import org.json.JSONObject;

import lombok.extern.slf4j.Slf4j;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;

import java.util.*;

/**
 * 监听的json数据转为kettle可以识别的形式
 */
@Slf4j
public class kettleTransform {


    public static kettleResponse createTransMeta(String json, String type, String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                                                 String originalUsername, String originalPassword) throws Exception {

        kettleResponse kettleResponse = new kettleResponse();
        KettleEnvironment.init();
        DatabaseMeta originalDbmeta = null; //
        try {
            originalDbmeta = new DatabaseMeta(originalDbname, originalDatabaseType, "Native(JDBC)", originalIp, originalDbname, originalPort, originalUsername, originalPassword);
        } catch (Exception e) {
            throw new Exception("" + e);
        }
        Database originalDatabase = new Database(originalDbmeta);
        originalDatabase.connect();


        try {
            if (json != null) {
                JSONObject jsonObj = new JSONObject(json);
                // 处理JSON数据
                if (jsonObj.has("table")) { //有时候为{}
                    String table = jsonObj.getString("table");
                    String sql = null;
                    if (originalDatabaseType.equals("ORACLE")) {
                        sql = "select * from " + originalSchema + "." + table + " where rownum <=10 ";  //用sql来获取字段名及属性以便在目标库中创建表
                    } else if (originalDatabaseType.equals("MSSQL")) {
                        sql = "select top 10 * from " + originalDatabase + "." + originalSchema + "." + table;   //sqlserver  没有limit 用top
                    } else {
                        sql = "select * from " + originalSchema + "." + table + "  limit 10;";
                    }

                    RowMetaInterface rowMetaInterface = originalDatabase.getQueryFieldsFromPreparedStatement(sql); //获取该表的字段结构
                    Object[] outputRowData = new Object[rowMetaInterface.size()];

                    if (type.equals("after") && jsonObj.keySet().contains("afterJson")) { //只输出after的
                        JSONObject sqlJsonObj = jsonObj.getJSONObject("afterJson");
                        getData(sqlJsonObj, rowMetaInterface, outputRowData);
                    }
                    if (type.equals("before") && jsonObj.keySet().contains("beforeJson")) { //只输出before的
                        JSONObject sqlJsonObj = jsonObj.getJSONObject("beforeJson");
                        getData(sqlJsonObj, rowMetaInterface, outputRowData);
                    }

                    kettleResponse.setOutputRowData(outputRowData);
                    kettleResponse.setOutputRowMeta(rowMetaInterface);
                    //            kettleLog.logBasic("-----------------" + rowMetaInterface);
                }

            }
        } finally {
            originalDatabase.disconnect();
        }
        return kettleResponse;
    }

    private static void getData(JSONObject sqlJsonObj, RowMetaInterface rowMetaInterface, Object[] outputRowData) {
        Iterator<String> keys = sqlJsonObj.keys();
        Map<String, Object> outputRowDataMap = new HashMap<>();

        while (keys.hasNext()) {
            String key = keys.next();
            Object value = sqlJsonObj.get(key);
            outputRowDataMap.put(key, value);
        }
        for (int i = 0; i < rowMetaInterface.size(); i++) {
            ValueMetaInterface valueMeta = rowMetaInterface.getValueMeta(i);
            outputRowData[i] = outputRowDataMap.get(valueMeta.getName());
        }
    }

}
