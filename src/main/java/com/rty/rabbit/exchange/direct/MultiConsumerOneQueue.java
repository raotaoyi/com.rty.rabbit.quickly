package com.rty.rabbit.exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * 一个队列多个消费者
 */
public class MultiConsumerOneQueue {
    public final static String EXCHANGE_NAME="direct_logs";

    private static class ConsumerWorker implements Runnable{
        final Connection connection;
        final String queueName;

        ConsumerWorker(Connection connection,String queueName){
            this.connection=connection;
            this.queueName=queueName;
        }
        @Override
        public void run() {
            try {
                //创建信道
                Channel channel = connection.createChannel();
                //在信道设置交换器
                channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
                //声明一个队列，rabbitMq，如果队列存在，不会再重复创建
                channel.queueDeclare(queueName,false,false,false,null);
                //消费者名字，打印输出用
                final String consumerName=Thread.currentThread().getName();
                //队列绑定到交换器时，是允许绑定多个路由键的，也就是多重绑定
                String[] routeKeys = {"king", "mark", "james"};
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
        connectionFactory.setHost("127.0.0.1");
        Connection connection=connectionFactory.newConnection();
        //3个线程，线程之间共享队列，一个队列，多个消费者
        String queueName="focusAll";
        for(int i=0;i<3;i++){
            Thread consumer=new Thread(new ConsumerWorker(connection,queueName));
            consumer.start();
        }
    }

}
