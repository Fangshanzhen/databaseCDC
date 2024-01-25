package com.fang.java.test;

import com.fang.java.oracleCDC;

public class oracleTest {
    public static void main(String[] args) throws Exception {

        /**
         * 如果首次测试的话需要删除偏移量D:\Debezium\offset\oracle下的2个文件
         * 监听时数据会有1-2分钟的延迟
         *
         */

//        oracleCDC.incrementData("ORACLE", "ORCL1", "C##TEST", "127.0.0.1", "1521", "c##test1",
//                "test1", "POSTGRESQL", "postgres", "test1", "127.0.0.1", "5432", "postgres",
//                "123456", "TEST", "10.0.108.51:9092", "debezium_oracle", null, null);




        oracleCDC.incrementData("ORACLE", "ORCL1", "C##FANG", "127.0.0.1", "1521", "c##fang",
                "test", "POSTGRESQL", "postgres", "test", "127.0.0.1", "5432", "postgres",
                "123456", "FANG", "10.0.108.51:9092", "oracle_cdc", "FANG", "FANG_INDEX","etltime");


    }
}