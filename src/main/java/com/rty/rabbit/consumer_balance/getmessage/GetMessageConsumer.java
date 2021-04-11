package com.rty.rabbit.consumer_balance.getmessage;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 类说明:消费的方式,拉取  消息的应答:自动确认和手动确认
 */
public class GetMessageConsumer {
    public static String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        //创建连接,连接RabbitMq
        ConnectionFactory connectionFactory = new ConnectionFactory();
        //设置工厂的连接地址,(localhost),默认使用的端口是5672
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        //创建信道
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        //声明一个队列
        String queueName = "focuserror";
        channel.queueDeclare(queueName, false, false, false, null);
        String routeKey = "error";
        channel.queueBind(queueName, EXCHANGE_NAME, routeKey);
        System.out.println("waiting for message......");
        //无限循环拉取
        while(true){
            //手动提交
            GetResponse response=channel.basicGet(queueName,false);
            if(null!=response){
                String message=new String(response.getBody(),"utf-8");
                System.out.println("received["+response.getEnvelope().getRoutingKey()+"]"+message);

            }
            //确认
            channel.basicAck(0,true);
            Thread.sleep(1000);
        }
    }
}
