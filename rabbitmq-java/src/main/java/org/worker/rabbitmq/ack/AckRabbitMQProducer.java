package org.worker.rabbitmq.ack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.worker.rabbitmq.RabbitMQConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class AckRabbitMQProducer {

    private static final String QUEUE_ACK_NAME = "DEMO_ACK_QUEUE";
    private static final String EXCHANGE_ACK_NAME = "DEMO_ACK_EXCHANGE";

    public static void main(String[] args) {

        ConnectionFactory factory = new ConnectionFactory();

        try {
            factory.setUri(RabbitMQConfig.RABBITMQ_URI);
            //建立连接
            Connection connection = factory.newConnection();
            //创建消息通道
            Channel channel = connection.createChannel();
            //声明队列
            channel.queueDeclare(QUEUE_ACK_NAME, true, false, false, null);

            //发送消息
            String msg = "hello ack message";
            String msg1 = "hello ack message, reject";
            String msg2 = "hello ack message, rejects";

            channel.basicPublish(EXCHANGE_ACK_NAME, "demo.ack", null, msg.getBytes());
            channel.basicPublish(EXCHANGE_ACK_NAME, "demo.ack", null, msg1.getBytes());
            channel.basicPublish(EXCHANGE_ACK_NAME, "demo.ack", null, msg2.getBytes());

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
        }
    }
}
