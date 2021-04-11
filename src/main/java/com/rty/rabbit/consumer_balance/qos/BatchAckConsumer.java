package com.rty.rabbit.consumer_balance.qos;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import javax.swing.*;
import java.io.IOException;

public class BatchAckConsumer extends DefaultConsumer {
    private int messageCount=0;
    public BatchAckConsumer(Channel channel) {
        super(channel);
        System.out.println("批量消费者启动了......");
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "utf-8");
        messageCount++;
        if(messageCount%50==0){
            this.getChannel().basicAck(envelope.getDeliveryTag(),true);
            System.out.println("batch consumer");
        }
        if(message.equals("stop")){
            //如果是最后一条消息就把剩余的消息进行确认
            this.getChannel().basicAck(envelope.getDeliveryTag(),true);
        }
    }
}
