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



//
//    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws Exception {
//        if (first) {
//
//            first = false;
//        }
//
//
//        Object[] r = getRow();
//
//
//        if (r == null) {
//            setOutputDone();
//            return false;
//
//        }
//
//
//
//        String json = get(Fields.In, "Message").getString(r);
//        if(json!=null){
//            kettleResponse res=kafkaTransform.createTransMeta(json);
//
//            putRow(res.getOutputRowMeta(), res.getOutputRowData());
//
//        }
//        return true;
//
//
//    }
//



}

