package org.worker.rabbitmq.confirm;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.worker.rabbitmq.RabbitMQConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class NormalConfirmProducer {

    private static final String QUEUE_CONFIRM_NAME = "DEMO_CONFIRM_QUEUE";
    private static final String EXCHANGE_CONFIRM_NAME = "DEMO_CONFIRM_EXCHANGE";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(RabbitMQConfig.RABBITMQ_URI);
            //建立连接
            Connection connection = factory.newConnection();
            //创建信道
            Channel channel = connection.createChannel();

            //声明队列
            channel.queueDeclare(QUEUE_CONFIRM_NAME, true, false, false, null);

            //开启发送方确认模式
            channel.confirmSelect();

            String msg = "hello confirm rabbitmq";

            //发送消息
            channel.basicPublish(EXCHANGE_CONFIRM_NAME, "demo.confirm", null, msg.getBytes());

            //普通confirm，发送一条确认一条
            if(channel.waitForConfirms()) {
                System.out.println("message sending succeed!");
            }else {
                System.out.println("message sending failed!");
            }

        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
