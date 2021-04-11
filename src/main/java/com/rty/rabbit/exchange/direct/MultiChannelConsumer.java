package com.rty.rabbit.exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * 一个连接多个信道
 */
public class MultiChannelConsumer {
    public final static String EXCHANGE_NAME="direct_logs";
    private static class ConsumerWorker implements Runnable{
        final Connection connection;
        public ConsumerWorker(Connection connection){
            this.connection=connection;
        }

        @Override
        public void run() {
            try {
                //创建信道
                Channel channel = connection.createChannel();
                //在信道设置交换器
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
                //声明队列放在消费者中去创建
                //绑定,将队列(queue-king)与交换器通过路由器绑定(king)
                String[] routeKeys = {"king", "mark", "james"};
                String queueName = channel.queueDeclare().getQueue();
                //消费者名字，打印输出
                final String consumerName=Thread.currentThread().getName();
                Arrays.asList(routeKeys).stream().forEach(routeKey -> {
                    try {
                        channel.queueBind(queueName, EXCHANGE_NAME, routeKey);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                System.out.println("waiting "+consumerName+" * for message.......");
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
            }catch (IOException e){
                e.fillInStackTrace();
            }
        }
    }
    public static void main(String[] args) throws IOException, TimeoutException {
        //创建连接,连接RabbitMq
        ConnectionFactory connectionFactory=new ConnectionFactory();
        //设置工厂的连接地址,(localhost),默认使用的端口是5672
        connectionFactory.setHost("localhost");
        Connection connection=connectionFactory.newConnection();
        for(int i=0;i<2;i++){
            Thread worker=new Thread(new ConsumerWorker(connection));
            worker.start();
        }

    }
}
