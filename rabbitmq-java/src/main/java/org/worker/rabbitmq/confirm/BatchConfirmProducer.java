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

public class BatchConfirmProducer {

    private static final String QUEUE_CONFIRM_NAME = "DEMO_CONFIRM_QUEUE";
    private static final String EXCHANGE_CONFIRM_NAME = "DEMO_CONFIRM_EXCHANGE";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();

        try {
            factory.setUri(RabbitMQConfig.RABBITMQ_URI);
            //建立连接
            Connection connection = factory.newConnection();
            //创建消息通道
            Channel channel = connection.createChannel();
            //声明队列
            channel.queueDeclare(QUEUE_CONFIRM_NAME, true, false, false,null);
            //发送消息
            String msg = "hello batch confirm rabbitmq";
            channel.confirmSelect();
            for(int i = 0; i < 5; i++) {
                channel.basicPublish(EXCHANGE_CONFIRM_NAME, QUEUE_CONFIRM_NAME, null, msg.getBytes());
            }

            //批量确认
            channel.waitForConfirmsOrDie();

            System.out.println("message sending succeed");
            channel.close();
            connection.close();

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
        }finally {

        }
    }
}
