package org.worker.rabbitmq.demo;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQConsumer {
    private static final String EXCHANGE_NAME = "DEMO_DIRECT_EXCHANGE";
    private static final String QUEUE_NAME = "DEMO_QUEUE";

    public static void main(String[] args) {
        //连接工厂类
        ConnectionFactory factory = new ConnectionFactory();
        //设置IP
        factory.setHost("127.0.0.1");
        //设置端口
        factory.setPort(5672);
        //设置virtual host
        factory.setVirtualHost("/");

        //设置用户密码
        factory.setUsername("guest");
        factory.setPassword("guest");


        try {
            //建立连接
            Connection connection = factory.newConnection();
            //建立通道
            Channel channel = connection.createChannel();

            //声明交换机
            channel.exchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);
            //声明队列
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            //绑定交换机和队列
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "demo.direct");


            //创建消费者
            Consumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body, "UTF-8");
                    System.out.println("receive msg: " + msg);
                    System.out.println("consumer tag: " + consumerTag);
                    System.out.println("delivery tag: " + envelope.getDeliveryTag());
                }
            };

            System.out.println("waiting to consuming...");
            //开始消费
            channel.basicConsume(QUEUE_NAME, true, consumer);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }


    }
}
