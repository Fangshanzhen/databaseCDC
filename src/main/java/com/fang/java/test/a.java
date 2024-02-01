//package com.fang.java.test;
//
//
//import com.alibaba.fastjson.JSONObject;
//
//public class a {
//
//
//
//
//
//
//import com.kettle.demo.utils.*;
//import com.fang.java.*;
//import java.util.regex.Matcher;
//import java.text.SimpleDateFormat;
//import java.util.*;
//import java.util.regex.Pattern;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingQueue;
//import com.alibaba.fastjson.JSONObject;
//
//    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws Exception {
//
//        if (first) {
//            first = false;
//        }
//
//        Object[] r = getRow();
//
//        if (r == null) {
//            setOutputDone();
//            return false;
//        }
//
//        r = createOutputRow(r, data.outputRowMeta.size());
//
//        String originalDatabaseType = get(Fields.In, "originalDatabaseType").getString(r);
//        String originalDbname = get(Fields.In, "originalDbname").getString(r);
//        String originalSchema = get(Fields.In, "originalSchema").getString(r);
//        String originalIp = get(Fields.In, "originalIp").getString(r);
//        String originalPort = get(Fields.In, "originalPort").getString(r);
//        String originalUsername = get(Fields.In, "originalUsername").getString(r);
//        String originalPassword = get(Fields.In, "originalPassword").getString(r);
//
//        String tableList = get(Fields.In, "tableList").getString(r);
//        String offsetAddress = get(Fields.In, "offsetAddress").getString(r);
//
//        String databaseHistoryAddress = get(Fields.In, "databaseHistoryAddress").getString(r);
//
//        BlockingQueue queue = new LinkedBlockingQueue();
//
//
//        cdc2queue.cdcData(originalDatabaseType, originalDbname, originalSchema, originalIp, originalPort,
//                originalUsername, originalPassword, tableList,
//                offsetAddress, databaseHistoryAddress, null, queue);
//
//
//        try {
//            JSONObject operateJson;
//            while ((operateJson = (JSONObject)queue.take()) != null) {
//                // 用从队列中获取的JSON对象设置输出行
//                if(operateJson.keySet().size()>0) {
//                    get(Fields.Out, "operateJson").setValue(r, String.valueOf(operateJson));
//                    // 将输出行传递给下一个步骤
//                    putRow(data.outputRowMeta, r);
//                }
//
//                // r = createOutputRow(getRow(), data.outputRowMeta.size());
//                // if (r == null) {
//                //      setOutputDone();
//                //      return false;
//                // }
//            }
//        } catch (KettleValueException e) {
//            logError("Error when setting field value", e);
//            setErrors(1);
//            stopAll();
//            return false;
//        }
//
//
//        return true;
//    }
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//}