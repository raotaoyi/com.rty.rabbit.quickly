package com.rty.rabbit.producer_balance.confirm;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 类说明:生产者--发送方确认模式---异步监听确认
 */
public class ProducerConfirmAsync {
    public final static String EXCHANGE_NAME="producer_async_confirm";

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
        //启动发送者确认模式
        channel.confirmSelect();
        //添加发送者确认添加器
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("send_ack"+deliveryTag+",multiple"+multiple);
            }

            @Override
            public void handleNack(long l, boolean b) throws IOException {

            }
        });
        //添加失败者通知
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routeKey, AMQP.BasicProperties basicProperties, byte[] body) throws IOException {
                String message=new String(body,"utf-8");
                System.out.println("rabbitMq路由失败"+routeKey+","+message);
            }
        });
        String[] routeKeys={"king","mark"};
        for(int i=0;i<6;i++){
            String routeKey=routeKeys[i%2];
            //发送的消息
            String message="Hello_world_"+(i+1)+"_"+System.currentTimeMillis();
            channel.basicPublish(EXCHANGE_NAME, routeKey, true, MessageProperties.PERSISTENT_BASIC, message.getBytes());
        }
/*        channel.close();
        connection.close();*/
    }
}
