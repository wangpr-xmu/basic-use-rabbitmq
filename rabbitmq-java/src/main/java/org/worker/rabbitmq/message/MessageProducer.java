package org.worker.rabbitmq.message;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.worker.rabbitmq.RabbitMQConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class MessageProducer {
    private static final String DEMO_MESSAGE_QUEUE = "DEMO_MESSAGE_QUEUE";
    private static final String DEMO_MESSAGE_EXCHANGE = "DEMO_MESSAGE_EXCHANGE";

    public static void main(String[] args) {

        ConnectionFactory factory = new ConnectionFactory();

        try {
            factory.setUri(RabbitMQConfig.RABBITMQ_URI);
            //建立连接
            Connection connection = factory.newConnection();
            //创建消息通道
            Channel channel = connection.createChannel();
            //声明队列
            channel.queueDeclare(DEMO_MESSAGE_QUEUE, true, false, false, null);

            Map<String, Object> headers = new HashMap<String, Object>();
            headers.put("name", "feichen");
            headers.put("level", "top");

            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .deliveryMode(2)   // 2代表持久化
                    .contentEncoding("UTF-8")  // 编码
                    .expiration("10000")  // TTL，过期时间
                    .headers(headers) // 自定义属性
                    .priority(5) // 优先级，默认为5，配合队列的 x-max-priority 属性使用
                    .messageId(String.valueOf(UUID.randomUUID()))
                    .build();
            //发送消息
            String msg = "hello message";

            channel.basicPublish(DEMO_MESSAGE_EXCHANGE, "demo.message", properties, msg.getBytes());

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
