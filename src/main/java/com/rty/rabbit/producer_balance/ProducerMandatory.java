package com.rty.rabbit.producer_balance;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ProducerMandatory {
    public final static String EXCHANGE_NAME = "balance_logs";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        //创建连接,连接RabbitMq
        ConnectionFactory connectionFactory = new ConnectionFactory();
        //设置工厂的连接地址,(localhost),默认使用的端口是5672
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        //创建信道
        Channel channel = connection.createChannel();
        //在信道设置交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        //连接关闭时执行 回调
        //信道关闭时执行 回调
        //失败通知 回调
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routeKey, AMQP.BasicProperties basicProperties, byte[] bytes) throws IOException {
                String message = new String(bytes, "utf-8");
                System.out.println("返回值replyCode:" + replyCode);
                System.out.println("返回值replyText:" + replyText);
                System.out.println("返回值exchange:" + exchange);
                System.out.println("返回值routeKey:" + routeKey);
            }
        });

        String[] routeKeys = {"king", "mark", "james"};
        for (int i = 0; i < 3; i++) {
            String routeKey = routeKeys[i % 3];
            String message = "hello word_" + (i + 1) + ("_" + System.currentTimeMillis());
            //开启回调监听
            channel.basicPublish(EXCHANGE_NAME, routeKey, true,null, message.getBytes());
            System.out.println("--------------------");
            System.out.println("[x] Sent" + routeKey + ":" + message);
            Thread.sleep(200);
        }
        channel.close();
        connection.close();
    }
}
