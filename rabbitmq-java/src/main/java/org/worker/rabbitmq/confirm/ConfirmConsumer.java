package org.worker.rabbitmq.confirm;

import com.rabbitmq.client.*;
import org.worker.rabbitmq.RabbitMQConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class ConfirmConsumer {

    private static final String QUEUE_CONFIRM_NAME = "DEMO_CONFIRM_QUEUE";
    private static final String EXCHANGE_CONFIRM_NAME = "DEMO_CONFIRM_EXCHANGE";

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
            channel.exchangeDeclare(EXCHANGE_CONFIRM_NAME, RabbitMQConfig.RABBITMQ_ROUTING_TYPE_DIRECT);
            //声明队列
            channel.queueDeclare(QUEUE_CONFIRM_NAME, true, false, false, null);
            //绑定队列和交换机
            channel.queueBind(QUEUE_CONFIRM_NAME, EXCHANGE_CONFIRM_NAME, "demo.confirm");


            Consumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body, "UTF-8");
                    System.out.println("receive message: " + msg);
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            };


            channel.basicConsume(QUEUE_CONFIRM_NAME, false, consumer);


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
