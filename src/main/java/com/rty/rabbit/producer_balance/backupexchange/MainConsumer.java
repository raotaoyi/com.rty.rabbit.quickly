package com.rty.rabbit.producer_balance.backupexchange;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

public class MainConsumer {
    public static String EXCHANGE_NAME = "main_exchange";
    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接,连接RabbitMq
        ConnectionFactory connectionFactory = new ConnectionFactory();
        //设置工厂的连接地址,(localhost),默认使用的端口是5672
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        //创建信道
        Channel channel = connection.createChannel();
        //备用交换器
        //channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT,false,false,null);
        //声明一个队列
        String queueName="backupexchange";
        channel.queueDeclare(queueName,false,false,false,null);
        String routeKey="king";//只关注king老师的日志，然后记录到文件中
        channel.queueBind(queueName,EXCHANGE_NAME,routeKey);
        System.out.println("[*] waiting for message.......");
        //创建队列消费者
        final Consumer consumer=new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String var1, Envelope envelope,AMQP.BasicProperties var3, byte[] bytes) throws UnsupportedEncodingException {
                String message=new String(bytes,"utf-8");
                System.out.println("received["+envelope.getRoutingKey()+"]"+message);
            }
        };
        //自动提交
        channel.basicConsume(queueName,true,consumer);
    }
}
