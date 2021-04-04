package com.rty.rabbit.exchange.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * 类说明:
 */
public class TopicProducer {
    public final static String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接,连接RabbitMq
        ConnectionFactory connectionFactory = new ConnectionFactory();
        //设置工厂的连接地址,(localhost),默认使用的端口是5672
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        //创建信道
        Channel channel = connection.createChannel();
        //在信道设置交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        String[] teachers = {"king", "mark", "james"};
        for (int i = 0; i < 3; i++) {
            String[] modules={"kafka","jvm","redis"};
            for(int j=0;j<3;j++) {
                String[] servers = {"A", "B", "C"};
                for (int k = 0; k < 3; k++) {
                    //发送的消息
                    String message = "hello topic_[" + i + "," + j + "," + k + "]";
                    String routeKey = teachers[i % 3] + "." + modules[j%3]+"."+servers[k%3];
                    channel.basicPublish(EXCHANGE_NAME,routeKey,null,message.getBytes());
                    System.out.println("[x] Sent"+routeKey+":"+message);
                }
            }
        }
        //
        channel.close();
        connection.close();
    }
}
