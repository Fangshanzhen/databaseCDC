package com.fang.java;

import org.json.JSONObject;

import lombok.extern.slf4j.Slf4j;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;

import java.util.*;

@Slf4j
public class kafkaTransform {
    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("消费kafka");

    public static kettleResponse createTransMeta(String json,String type) throws Exception {

        kettleResponse kettleResponse = new kettleResponse();

        JSONObject jsonObj = new JSONObject(json);
        // 处理JSON数据
        JSONObject sqlJsonObj = jsonObj.getJSONObject("sqlJson");

        Iterator<String> keys = sqlJsonObj.keys();

        RowMetaInterface outputRowMeta = new RowMeta();
        Map<String, Object> outputRowDataMap = new HashMap<>();

        while (keys.hasNext()) {
            String key = keys.next();
            Object value = sqlJsonObj.get(key);
            ValueMetaInterface valueMeta = new ValueMetaString(key);
            outputRowMeta.addValueMeta(valueMeta);
            outputRowDataMap.put(key, value);
        }

        // Create output row
        Object[] outputRowData = new Object[outputRowMeta.size()];
        for (int i = 0; i < outputRowMeta.size(); i++) {
            ValueMetaInterface valueMeta = outputRowMeta.getValueMeta(i);
            outputRowData[i] = outputRowDataMap.get(valueMeta.getName());
        }
        kettleResponse.setOutputRowData(outputRowData);
        kettleResponse.setOutputRowMeta(outputRowMeta);


        kettleLog.logBasic("-----------------" + outputRowMeta);
        return kettleResponse;
    }

}
