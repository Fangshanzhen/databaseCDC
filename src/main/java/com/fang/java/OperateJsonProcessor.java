package com.fang.java;

import com.alibaba.fastjson.JSONObject;
import java.util.concurrent.BlockingQueue;


public  class OperateJsonProcessor implements Runnable {
    private BlockingQueue<JSONObject> queue;

    public OperateJsonProcessor(BlockingQueue<JSONObject> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                JSONObject operateJson = queue.take(); // 会阻塞直到队列中有元素
                processOperateJson(operateJson);
            } catch (Exception e) {
                Thread.currentThread().interrupt(); // 重设中断标志
                break;
            }
        }
    }

    public static void  processOperateJson(JSONObject operateJson) throws Exception {

        System.out.println("收到监听的消息： " + operateJson);
        //todo 处理operateJson


    }
}

