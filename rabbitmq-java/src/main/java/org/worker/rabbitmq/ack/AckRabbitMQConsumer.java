package org.worker.rabbitmq.ack;

import com.rabbitmq.client.*;
import org.worker.rabbitmq.RabbitMQConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class AckRabbitMQConsumer {
    private static final String QUEUE_ACK_NAME = "DEMO_ACK_QUEUE";
    private static final String EXCHANGE_ACK_NAME = "DEMO_ACK_EXCHANGE";

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
            channel.exchangeDeclare(EXCHANGE_ACK_NAME, RabbitMQConfig.RABBITMQ_ROUTING_TYPE_DIRECT);
            //声明队列
            channel.queueDeclare(QUEUE_ACK_NAME, true, false, false, null);
            //绑定队列和交换机
            channel.queueBind(QUEUE_ACK_NAME, EXCHANGE_ACK_NAME, "demo.ack");


            Consumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body, "UTF-8");
                    System.out.println("receive message: " + msg);
                    if(msg.contains("reject")) {
                        //拒绝消息
                        //true: 重新入队列，false: 直接丢弃
                        channel.basicReject(envelope.getDeliveryTag(), false);
                    }else if(msg.contains("rejects")) {
                        //批量拒绝
                        channel.basicNack(envelope.getDeliveryTag(), true, false);
                    }else {
                        //手工应答
                        channel.basicAck(envelope.getDeliveryTag(), true);

                    }
                }
            };


            channel.basicConsume(QUEUE_ACK_NAME, false, consumer);


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
