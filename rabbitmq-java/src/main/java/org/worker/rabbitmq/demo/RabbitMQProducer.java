package org.worker.rabbitmq.demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQProducer {

    private static final String EXCHANGE_NAME = "DEMO_DIRECT_EXCHANGE";

    public static void main(String[] args) {
        //RabbitMQ 创建连接工厂类
        ConnectionFactory factory = new ConnectionFactory();
        //设置IP
        factory.setHost("127.0.0.1");
        //设置端口
        factory.setPort(5672);
        //设置virtual host
        factory.setVirtualHost("/");

        //用户密码
        factory.setUsername("guest");
        factory.setPassword("guest");


        try {
            //建立连接
            Connection connection = factory.newConnection();
            //创建消息通道
            Channel channel = connection.createChannel();

            String msg = "hello world, rabbitmq";

            //发送消息
            channel.basicPublish(EXCHANGE_NAME, "demo.direct", null, msg.getBytes());

            //关闭连接
            channel.close();
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
