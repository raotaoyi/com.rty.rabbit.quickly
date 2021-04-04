package com.rty.rabbit.exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

/**
 * 类说明:普通的消费者
 */
public class NormalConsumer {
    public final static String EXCHANGE_NAME="direct_logs";
    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接,连接RabbitMq
        ConnectionFactory connectionFactory=new ConnectionFactory();
        //设置工厂的连接地址,(localhost),默认使用的端口是5672
        connectionFactory.setHost("localhost");
        Connection connection=connectionFactory.newConnection();
        //创建信道
        Channel channel=connection.createChannel();
        //在信道设置交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        //声明队列放在消费者中去创建
        String queueName="queue-king";
        channel.queueDeclare(queueName,false,false,false,null);
        //绑定,将队列(queue-king)与交换器通过路由器绑定(king)
        String routeKey="king";
        channel.queueBind(queueName,EXCHANGE_NAME,routeKey);
        System.out.println("waiting for message.......");
        //声明一个消费者
        final Consumer consumer=new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String var1, Envelope envelope,AMQP.BasicProperties var3, byte[] bytes) throws UnsupportedEncodingException {
                String message=new String(bytes,"utf-8");
                System.out.println("received["+envelope.getRoutingKey()+"]"+message);
            }
        };
        //消费者正式开始在指定的队列上消费
        channel.basicConsume(queueName,true,consumer);
    }
}
