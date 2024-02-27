package com.fang.java.test;


import com.fang.java.cdc.databaseCDC;
import com.fang.java.cdc.databaseCDC_database;
import com.fang.java.cdc.databaseCDC_queue;
import com.fang.java.cdc.queue1;

import java.util.concurrent.LinkedBlockingQueue;


public class test10 {


    public static void main(String[] args) throws Exception {


//        databaseCDC.cdcData("postgresql", "postgres", "test", "127.0.0.1", "5432", "postgres",
//                "123456","test2,test3","10.0.108.51:9092","postgresql_topic","D:\\Debezium\\offset\\postgresql\\file.dat",
//                "D:\\Debezium\\offset\\postgresql\\dbhistory.dat",null);


        /**
         *     * 如果首次测试的话需要删除偏移量D:\Debezium\offset\oracle下的2个文件
         *          oracle监听时数据会有1-2分钟的延迟
         */
//        databaseCDC.cdcData("oracle", "ORCL1", "C##FANG", "127.0.0.1", "1521", "c##fang",
//                "test","FANG","10.0.108.51:9092","oracle_topic","D:\\Debezium\\offset\\oracle\\file.dat",
//                "D:\\Debezium\\offset\\oracle\\dbhistory.dat",null);


        /**
         * express服务的版本不支持SQL Server 代理，因此无法进行cdc监控
         * 错误 "无法对数据库 'master' 启用变更数据捕获。系统数据库或分发数据库不支持变更数据捕获。"
         * SQL Server不允许在系统数据库上启用CDC。系统数据库包括 master、model、msdb 和 tempdb 数据库。CDC只能在用户定义的数据库上启用
         * 本地新建sqlserver数据库mydatabase进行测试成功
         */
//
//        databaseCDC.cdcData("sqlserver", "mydatabase", "dbo", "127.0.0.1", "1433", "sa",
//                "123456", "fangtest", "10.0.108.51:9092", "sqlserver_topic", "D:\\Debezium\\offset\\sqlserver\\file.dat",
//                "D:\\Debezium\\offset\\sqlserver\\dbhistory.dat", null);

//
//        databaseCDC.cdcData("mysql","test", "test", "127.0.0.1", "3306", "root",
//                "123456","test2","10.0.108.51:9092","mysql_topic","D:\\Debezium\\offset\\mysql\\file.dat",
//                "D:\\Debezium\\offset\\mysql\\dbhistory.dat","1222");


        databaseCDC_queue.cdcData("postgresql", "postgres", "test", "127.0.0.1", "5432", "postgres",
                "123456", "test2,test3", "D:\\Debezium\\offset\\postgresql\\file.dat",
                "D:\\Debezium\\offset\\postgresql\\dbhistory.dat", null, new LinkedBlockingQueue(),"debezium_1234");


//        databaseCDC_database.cdcData("ORACLE", "ORCL1", "C##FANG", "127.0.0.1", "1521", "c##fang",
//                "test","FANG,FANG1","D:\\Debezium\\offset\\oracle\\file.dat","D:\\Debezium\\offset\\oracle\\dbhistory.dat","1222",
//                "POSTGRESQL","postgres","test","127.0.0.1","5432","postgres","123456","AA#BB,FANG"
//                ,"aa_index","etltime","debezium1");


    }

}







