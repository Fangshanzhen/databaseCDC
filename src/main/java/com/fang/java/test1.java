package com.fang.java;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static com.fang.java.CDCUtils.*;

public class test1 {


//    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
//        // 获取转换数据
//        UserDefinedJavaClassData data = (UserDefinedJavaClassData) sdi;
//
//        // 如果是第一次处理行
//        if (first) {
//            first = false;
//
//            // 获取输入行的元数据
//            data.outputRowMeta = getInputRowMeta().clone();
//        }
//
//        // 从上一个步骤获取一行数据
//        Object[] inputRow = getRow();
//
//        // 如果没有更多的数据，完成该步骤
//        if (inputRow == null) {
//            setOutputDone();
//            return false;
//        }
//
//        // 从当前行的Message字段获取JSON字符串
//        String json = get(Fields.In, "Message").getString(inputRow);
//        try {
//            if (json != null && !json.isEmpty()) {
//                // 解析JSON并创建转换元数据和输出行数据
//
//                kettleResponse res = kafkaTransform.createTransMeta(json, "1");
//
//                if(res!=null) {
//
//                    // 在第一次调用时，更新outputRowMeta来增加新字段
//                    if (data.outputRowMeta.size() == getInputRowMeta().size()) { // 确保仅更新一次
//                        // 假设createTransMeta已经包含了新增字段的处理逻辑
//                        if(res.getOutputRowMeta()!=null) {
//                            data.outputRowMeta.addRowMeta(res.getOutputRowMeta());
//                        }
//                    }
//
//                    // 创建新的输出行数组，大小为动态元数据的字段数量
//                    Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
//                    if(outputRow!=null) {
//
//                        // 填充原始输入数据
//                        for (int i = 0; i < getInputRowMeta().size(); i++) {
//                            outputRow[i] = inputRow[i];
//                        }
//
//                        // 填充新添加的字段数据
//                        Object[] addedFieldsData = res.getOutputRowData();
//                        for (int i = 0; i < addedFieldsData.length; i++) {
//                            outputRow[getInputRowMeta().size() + i] = addedFieldsData[i];
//                        }
//                    }
//
//                    // 向下一步传递处理后的行
//                    putRow(data.outputRowMeta, outputRow);
//                }
//            } else {
//                // 如果需要，这里可以记录错误或者进行其他操作
//                logError("Error parsing JSON from field 'Message': " + json);
//            }
//
//        } catch (Exception e) {
//            logError("Error processing Kafka transformation: " + e.getMessage());
//            setErrors(1);
//            stopAll();
//            setOutputDone();
//            return false;
//        }
//
//        return true; // 表示我们继续处理下一行数据
//    }


    public static void main(String[] args) throws Exception {

        String s="{\"database\":\"postgres\",\"operate_type\":\"update\",\"beforeJson\":{\"column1\":\"14\",\"column3\":\"1666\",\"column2\":\"14\"},\"afterJson\":{\"column1\":\"14\",\"column3\":\"1777\",\"column2\":\"14\"},\"operate_ms\":\"2024-01-22 14:34:41\",\"table\":\"test2\"}";
        kafkaTransform.createTransMeta(s,"before","POSTGRESQL", "postgres", "test", "127.0.0.1", "5432", "postgres", "123456");


    }


}

