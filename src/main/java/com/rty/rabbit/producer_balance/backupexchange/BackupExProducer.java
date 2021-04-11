package com.rty.rabbit.producer_balance.backupexchange;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class BackupExProducer {
    public static String EXCHANGE_NAME = "main_exchange";
    public static String BAK_EXCHANGE_NAME = "ae";
    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接,连接RabbitMq
        ConnectionFactory connectionFactory = new ConnectionFactory();
        //设置工厂的连接地址,(localhost),默认使用的端口是5672
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        //创建信道
        Channel channel = connection.createChannel();
        //声明备用交换器
        Map<String,Object> argsMap=new HashMap<>();
        argsMap.put("alternate-exchange",BAK_EXCHANGE_NAME);
        //主交换器
        channel.exchangeDeclare(EXCHANGE_NAME,BuiltinExchangeType.DIRECT,false,false,argsMap);
        //备用交换器
        channel.exchangeDeclare(BAK_EXCHANGE_NAME, BuiltinExchangeType.FANOUT,true,false,null);
        //所有的消息
        String[] routeKeys={"king","mark","james"};
        for(int i=0;i<3;i++){
            //每一次发送一条不同老师的信息
            String routeKey=routeKeys[i%3];
            //发送的消息
            String message="Hello_World_"+(i+1);
            channel.basicPublish(EXCHANGE_NAME, routeKey, null, message.getBytes());
            System.out.println("send message [" + routeKey + "]:" + message);
        }
        //关系频道和连接
        channel.close();
        connection.close();
    }
}
