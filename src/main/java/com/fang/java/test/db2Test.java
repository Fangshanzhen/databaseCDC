package com.fang.java.test;

import com.fang.java.db2CDC;

public class db2Test {
    public static void main(String[] args) throws Exception {




        db2CDC.incrementData("DB2", "SAMPLE", "FSZ", "127.0.0.1", "50000", "db2admin",
                "123456", "MYSQL", "test?useUnicode=true&characterEncoding=utf-8", "test", "127.0.0.1", "3308", "root",
                "123", "TESTDB2","10.0.108.51:9092","db2_cdc","DB_ID","db_index","etltime");  // postgresql2mysql
    }


}

