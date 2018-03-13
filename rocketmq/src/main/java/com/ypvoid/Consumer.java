package com.ypvoid;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by sheny on 2018/3/13.
 */
public class Consumer {
    public static void main(String[] args) {
        Consumer.receive();
    }
    public static void receive() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_demo");
        consumer.setNamesrvAddr("127.0.0.1:9876");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        try {
            consumer.subscribe("TopicTest", "*");
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                    for (MessageExt ext : list) {
                        String msg = null;
                        try {
                            msg = new String(ext.getBody(),"utf-8");
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                        System.out.println("MessageBody:" + msg);
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }

            });
            consumer.start();
            System.out.println("Consumer Started.");
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
