package com.fang.java.test;


import com.fang.java.cdc.databaseCDC;

public class mysqlTest {
    public static void main(String[] args) throws Exception {
                databaseCDC.cdcData("mysql","test", "test", "127.0.0.1", "3306", "root",
                "123456","fangtest1","10.0.108.51:9092","mysql_topic","D:\\Debezium\\offset\\mysql\\file.dat",
                "D:\\Debezium\\offset\\mysql\\dbhistory.dat","1222");
    }
}
