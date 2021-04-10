package org.worker.rabbitmq.limit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.worker.rabbitmq.RabbitMQConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class LimitProducer {

    private static final String DEMO_LIMIT_QUEUE = "DEMO_LIMIT_QUEUE";
    private static final String DEMO_LIMIT_EXCHANGE = "DEMO_LIMIT_EXCHANGE";

    public static void main(String[] args) {

        ConnectionFactory factory = new ConnectionFactory();

        try {
            factory.setUri(RabbitMQConfig.RABBITMQ_URI);
            //建立连接
            Connection connection = factory.newConnection();
            //创建消息通道
            Channel channel = connection.createChannel();
            //声明队列
            channel.queueDeclare(DEMO_LIMIT_QUEUE, true, false, false, null);

            //发送消息
            String msg = "hello limit message";

            for(int i = 0;i < 10; i++) {
                channel.basicPublish(DEMO_LIMIT_EXCHANGE, "demo.limit", null, msg.getBytes());
            }

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
