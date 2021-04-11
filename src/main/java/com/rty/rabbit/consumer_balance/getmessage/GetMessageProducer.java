package com.rty.rabbit.consumer_balance.getmessage;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class GetMessageProducer {
    public static String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接,连接RabbitMq
        ConnectionFactory connectionFactory = new ConnectionFactory();
        //设置工厂的连接地址,(localhost),默认使用的端口是5672
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        //创建信道
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        //所有日志的严重性级别
        for (int i = 0; i < 3; i++) {
            //发送的消息
            String message="Hello_World"+(i+1);
            channel.basicPublish(EXCHANGE_NAME, "error", null, message.getBytes());
            System.out.println("send message error " + message);
        }
        channel.close();
        connection.close();
    }
}
