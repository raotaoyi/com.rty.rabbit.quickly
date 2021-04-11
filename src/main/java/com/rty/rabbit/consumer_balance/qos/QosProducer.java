package com.rty.rabbit.consumer_balance.qos;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 类说明:发送消息(发送20条消息，其中第210条消息表示本批次的消息结束)
 */
public class QosProducer {
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
        //指定转发
        channel.exchangeDeclare("kajsdi",BuiltinExchangeType.DIRECT);
        //生产者发送非常多的数据
        //发送210条消息，其中第210条消息表示本批次的消息结束
        for (int i=0;i<210;i++){
            //发送的消息
            String message="Hello_world_"+(i+1);
            if(i==209){
                message="stop";
            }
            channel.basicPublish(EXCHANGE_NAME, "error", null, message.getBytes());
            System.out.println("send message error " + message);

        }
        channel.close();
        connection.close();
    }


}
