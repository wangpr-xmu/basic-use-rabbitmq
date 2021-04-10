package org.worker.rabbitmq.transaction;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.worker.rabbitmq.RabbitMQConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class TransactionProducer {
    private static final String QUEUE_TRANSACTION_NAME = "DEMO_TRANSACTION_QUEUE";
    private static final String DEMO_TRANSACTION_EXCHANGE = "DEMO_TRANSACTION_EXCHANGE";

    public static void main(String[] args) {

        ConnectionFactory factory = new ConnectionFactory();

        Connection connection = null;
        Channel channel = null;

        try {
            factory.setUri(RabbitMQConfig.RABBITMQ_URI);
            //建立连接
            connection = factory.newConnection();
            //创建消息通道
            channel = connection.createChannel();
            //声明队列
            channel.queueDeclare(QUEUE_TRANSACTION_NAME, true, false, false, null);

            //发送消息
            String msg = "hello ack message";
            //开启事务模式
            channel.txSelect();

            channel.basicPublish(DEMO_TRANSACTION_EXCHANGE, "demo.transaction", null, msg.getBytes());
//            channel.txCommit();

            channel.basicPublish(DEMO_TRANSACTION_EXCHANGE, "demo.transaction", null, msg.getBytes());
//            channel.txCommit();

            channel.basicPublish(DEMO_TRANSACTION_EXCHANGE, "demo.transaction", null, msg.getBytes());
//            channel.txCommit();

            int i = 1/0;

            channel.basicPublish(DEMO_TRANSACTION_EXCHANGE, "demo.transaction", null, msg.getBytes());
            channel.txCommit();

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
        } catch (Exception e) {
            try {
                channel.txRollback();
                System.out.println("message is rollback");
            } catch (IOException ex) {
                ex.printStackTrace();
            }

        } finally {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
