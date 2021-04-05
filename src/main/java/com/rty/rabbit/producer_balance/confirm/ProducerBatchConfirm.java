package com.rty.rabbit.producer_balance.confirm;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ProducerBatchConfirm {
    public static String EXCHANGE_NAME = "producer_wait_confirm";


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
        //添加失败通知监听器
        String ROUTE_KEY = "king";
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
        //启用发送者确认模式
        channel.waitForConfirms();
        for (int i = 0; i < 2; i++) {
            String message = "Hello world_" + (i + 1);
            channel.basicPublish(EXCHANGE_NAME, ROUTE_KEY, true, null, message.getBytes());
            System.out.println("send message [" + ROUTE_KEY + "]:" + message);
        }
        //启用发送者确认模式(批量确认)
        channel.waitForConfirmsOrDie();
        channel.close();
        connection.close();
    }
}
