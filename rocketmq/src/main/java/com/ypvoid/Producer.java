package com.ypvoid;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;

/**
 * Created by sheny on 2018/3/13.
 */
public class Producer {
    public static void main(String[] args) {
        Producer.sendMsg();
    }
    public static void sendMsg() {
        DefaultMQProducer producer = new DefaultMQProducer("producer_demo");
        producer.setNamesrvAddr("127.0.0.1:9876");
        try {
            producer.start();
            for (int i = 0; i < 100; i++) {
                Message msg = new Message("TopicTest", "TagA", ("测试RecketMq" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n", sendResult);
            }
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        }
        producer.shutdown();
    }
}
