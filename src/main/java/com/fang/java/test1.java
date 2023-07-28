package com.fang.java;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//dbhistory.dat

public class test1 {
    public static void main(String[] args) {
        List<String>fileList=new ArrayList<>();
        fileList.add("D:\\Debezium\\offset\\dbhistory.dat");
        fileList.add("D:\\Debezium\\offset\\file.dat");
        try {
            for(String s:fileList) {
                File file = new File(s);
                if (file.createNewFile()) {
                    System.out.println("File created: " + file.getName());
                } else {
                    System.out.println("File already exists.");
                }
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

    }
}
