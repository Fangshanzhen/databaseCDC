package com.fang.java.test;

import com.fang.java.cdc.databaseCDC_database;

public class postgresqlTest {

    public static void main(String[] args) throws Exception {


        databaseCDC_database.cdcData("POSTGRESQL", "postgres", "test", "127.0.0.1", "5432", "postgres",
                "123456", "test2", "D:\\\\Debezium\\\\offset\\\\postgresql\\\\file.dat", "D:\\\\Debezium\\\\offset\\\\postgresql\\\\dbhistory.dat", null,
                "MYSQL", "test?useUnicode=true&characterEncoding=utf-8", "test", "127.0.0.1", "3306", "root",
                "123456", "column1,column2", "x_index", "etltime","debezium");  // postgresql2mysql



//        databaseCDC_database.cdcData("POSTGRESQL", "postgres", "test", "127.0.0.1", "5432", "postgres",
//                "123456", "test2", "D:\\\\Debezium\\\\offset\\\\postgresql\\\\file.dat", "D:\\\\Debezium\\\\offset\\\\postgresql\\\\dbhistory.dat", null,
//                "POSTGRESQL", "postgres", "public", "127.0.0.1", "5432", "postgres",
//                "123456", "column1,column2", "x_index", "etltime");  // postgresql2postgresql




//        databaseCDC.cdcData("postgresql", "postgres", "test", "127.0.0.1", "5432", "postgres",
//                "123456","test2,test3","10.0.108.51:9092","postgresql_topic","D:\\Debezium\\offset\\postgresql\\file.dat",
//                "D:\\Debezium\\offset\\postgresql\\dbhistory.dat",null);

    }


}
