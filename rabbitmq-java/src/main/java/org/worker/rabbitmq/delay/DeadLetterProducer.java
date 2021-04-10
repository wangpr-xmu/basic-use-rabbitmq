package org.worker.rabbitmq.delay;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.worker.rabbitmq.RabbitMQConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class DeadLetterProducer {
    private static final String EXCHANGE_NAME = "DEMO_DIRECT_EXCHANGE_DEAD";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(RabbitMQConfig.RABBITMQ_URI);
            //创建连接
            Connection connection = factory.newConnection();
            //建立消息通道
            Channel channel = connection.createChannel();

            //设置消息属性
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .deliveryMode(2)
                    .contentEncoding("UTF-8")
                    .expiration("10000")
                    .build();

            String msg = "hello delayed message";

            channel.basicPublish("DEMO_DIRECT_EXCHANGE_DEAD", "demo.dead", properties, msg.getBytes());

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
