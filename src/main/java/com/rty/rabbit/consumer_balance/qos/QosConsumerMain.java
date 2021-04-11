package com.rty.rabbit.consumer_balance.qos;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

public class QosConsumerMain {
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
        //声明一个队列
        String queueName="focuserror";
        channel.queueDeclare(queueName,false,false,false,null);
        //绑定，将队列和交换器通过路由键进行绑定
        String routeKey="error";
        channel.queueBind(queueName,EXCHANGE_NAME,routeKey);
        System.out.println("waiting for message......");
        //声明一个消费者
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String var1, Envelope envelope, AMQP.BasicProperties var3, byte[] bytes) throws IOException {
                String message = new String(bytes, "utf-8");
                System.out.println("received[" + envelope.getRoutingKey() + "]" + message);
                channel.basicAck(envelope.getDeliveryTag(),false);
            }
        };
        //150条预取
        channel.basicQos(150,true);
        //消息正是开始从指定队列中消费消息
        channel.basicConsume(queueName,false,consumer);
        //自定义消费者批量确认
        BatchAckConsumer batchAckConsumer=new BatchAckConsumer(channel);
        channel.basicConsume(queueName,false,batchAckConsumer);
    }
}
