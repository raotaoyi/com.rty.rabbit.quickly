package com.rty.rabbit.exchange.fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * 类说明:fanout类型交换器的生产者
 */
public class FanoutProducer {
    public final static String EXCHANGE_NAME="fanout_logs";
    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接,连接RabbitMq
        ConnectionFactory connectionFactory=new ConnectionFactory();
        //设置工厂的连接地址,(localhost),默认使用的端口是5672
        connectionFactory.setHost("localhost");
        Connection connection=connectionFactory.newConnection();
        //创建信道
        Channel channel=connection.createChannel();
        //在信道设置交换器
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        String[] routeKeys={"king","mark","james"};
        Arrays.asList(routeKeys).stream().forEach(routeKey->{
            String msg="hello,RabbitMQ"+routeKey;
            System.out.println(msg);
            //发布消息
            try {
                channel.basicPublish(EXCHANGE_NAME,routeKey,null,msg.getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        channel.close();
        connection.close();
    }
}
