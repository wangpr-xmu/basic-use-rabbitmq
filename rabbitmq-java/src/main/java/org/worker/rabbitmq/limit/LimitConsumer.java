package org.worker.rabbitmq.limit;

import com.rabbitmq.client.*;
import org.worker.rabbitmq.RabbitMQConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class LimitConsumer {

    private static final String DEMO_LIMIT_QUEUE = "DEMO_LIMIT_QUEUE";
    private static final String DEMO_LIMIT_EXCHANGE = "DEMO_LIMIT_EXCHANGE";

    public static void main(String[] args) {
        //客户端工厂
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(RabbitMQConfig.RABBITMQ_URI);
            //建立连接
            Connection connection = factory.newConnection();
            //建立通道
            final Channel channel = connection.createChannel();
            //声明交换机
            channel.exchangeDeclare(DEMO_LIMIT_EXCHANGE, RabbitMQConfig.RABBITMQ_ROUTING_TYPE_DIRECT);
            //声明队列
            channel.queueDeclare(DEMO_LIMIT_QUEUE, true, false, false, null);
            //绑定队列和交换机
            channel.queueBind(DEMO_LIMIT_QUEUE, DEMO_LIMIT_EXCHANGE, "demo.limit");

            Consumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body, "UTF-8");
                    System.out.println("receive message: " + msg);
                    //手工应答
                    channel.basicAck(envelope.getDeliveryTag(), true);
                }
            };

            channel.basicQos(2);
            channel.basicConsume(DEMO_LIMIT_QUEUE, false, consumer);


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
