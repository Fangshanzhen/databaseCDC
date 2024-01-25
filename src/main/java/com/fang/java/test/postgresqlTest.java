package com.fang.java.test;

import com.fang.java.*;

public class postgresqlTest {

    public static void main(String[] args) throws Exception {


//        postgresqlCDC.incrementData("POSTGRESQL", "postgres", "test", "127.0.0.1", "5432", "postgres",
//                "123456", "MYSQL", "test?useUnicode=true&characterEncoding=utf-8", "test", "127.0.0.1", "3308", "root",
//                "123", "test2","10.0.108.51:9092","postgresql_cdc","column1,column2","x_index","etltime");  // postgresql2mysql




        postgresqlCDCnew.incrementData("POSTGRESQL", "postgres", "test", "127.0.0.1", "5432", "postgres",
                "123456","test2","10.0.108.51:9092","test01191","D:\\Debezium\\offset\\postgresql\\file.dat",
                "D:\\Debezium\\offset\\postgresql\\dbhistory.dat");



    }





}
