package com.fang.java.test;

import com.fang.java.sqlserverCDC;

public class sqlserverTest {
    public static void main(String[] args) throws Exception {


        /**
         * 如果首次测试的话需要删除偏移量   D:\Debezium\offset\sqlserver   下的2个文件
         */


//        sqlserverCDC.incrementData("MSSQL", "mydatabase", "dbo", "127.0.0.1", "1433", "sa",
//                "123456", "POSTGRESQL", "postgres", "test", "127.0.0.1", "5432", "postgres",
//                "123456", "test", "10.0.108.51:9092", "sqlserver_cdc", null, null);



        sqlserverCDC.incrementData("MSSQL", "mydatabase", "dbo", "127.0.0.1", "1433", "sa",
                "123456", "POSTGRESQL", "postgres", "test", "127.0.0.1", "5432", "postgres",
                "123456", "fangtest", "10.0.108.51:9092", "sqlserver_cdc", "aa", "index_aa","etltime");

    }
}