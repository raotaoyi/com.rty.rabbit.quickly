package com.rty.rabbit.producer_balance.confirm;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

/**
 * 类说明:消费者-发送方确认模式
 */
public class ProducerConfirmConsumer {
    public final static String EXCHANGE_NAME = "balance_confirm_logs";

    public final static String ROUTE_KEY = "king";

    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接,连接RabbitMq
        ConnectionFactory connectionFactory = new ConnectionFactory();
        //设置工厂的连接地址,(localhost),默认使用的端口是5672
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        //创建信道
        Channel channel = connection.createChannel();
        //在信道设置交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = "confirm_queue";
        channel.queueDeclare(queueName, false, false, false, null);
        //只关注king
        channel.queueBind(queueName,EXCHANGE_NAME,ROUTE_KEY);
        System.out.println("[*] waiting for message......");
        final Consumer consumer=new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,AMQP.BasicProperties var3, byte[] bytes) throws UnsupportedEncodingException {
                String message=new String(bytes,"utf-8");
                System.out.println("received["+envelope.getRoutingKey()+"]"+message);
            }
        };
        //消费者正式开始在指定的队列上消费
        channel.basicConsume(queueName,true,consumer);
    }
}
