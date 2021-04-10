package org.worker.rabbitmq.delay;

import com.rabbitmq.client.*;
import org.worker.rabbitmq.RabbitMQConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class DeadLetterConsumer {
    private static final String EXCHANGE_NAME = "DEMO_DIRECT_EXCHANGE_DEAD";
    private static final String QUEUE_NAME = "DEMO_QUEUE_DEAD";


    private static final String EXCHANGE_DEAD_LETTER_NAME = "DEMO_DIRECT_EXCHANGE_DEAD_LETTER";
    private static final String QUEUE_DEAD_LETTER_NAME = "DEMO_QUEUE_DEAD_LETTER";


    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(RabbitMQConfig.RABBITMQ_URI);
            //建立连接
            Connection connection = factory.newConnection();
            //建立消息通道
            Channel channel = connection.createChannel();

            //设置队列参数
            Map<String, Object> argument = new HashMap<>();
            //指定死信队列交换机
            argument.put("x-dead-letter-exchange", EXCHANGE_DEAD_LETTER_NAME);
            // argument.put("x-expires",9000L); // 设置队列的TTL
            // argument.put("x-max-length", 4); // 如果设置了队列的最大长度，超过长度时，先入队的消息会被发送到DLX
            //声明队列
            channel.queueDeclare(QUEUE_NAME, true, false, false, argument);
            //声明交换机
            channel.exchangeDeclare(EXCHANGE_NAME, "direct", true, false, false, null);
            //绑定队列和交换机
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "demo.dead");

            //声明死信交换机
            channel.exchangeDeclare(EXCHANGE_DEAD_LETTER_NAME, "topic", false, false, false, null);
            //声明死信队列
            channel.queueDeclare(QUEUE_DEAD_LETTER_NAME, false, false, false, null);

            //绑定
            channel.queueBind(QUEUE_DEAD_LETTER_NAME, EXCHANGE_DEAD_LETTER_NAME, "#");

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body, "UTF-8");
                    System.out.println("received message: " + msg);
                }
            };

            channel.basicConsume(QUEUE_DEAD_LETTER_NAME, true, consumer);

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
