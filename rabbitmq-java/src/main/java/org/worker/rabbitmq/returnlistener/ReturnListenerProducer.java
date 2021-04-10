package org.worker.rabbitmq.returnlistener;

import com.rabbitmq.client.*;
import org.worker.rabbitmq.RabbitMQConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ReturnListenerProducer {
    private static final String DEMO_RETURN_QUEUE = "DEMO_RETURN_QUEUE";
    private static final String DEMO_RETURN_EXCHANGE = "DEMO_RETURN_EXCHANGE";

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

            //消息退回监听器
            channel.addReturnListener(new ReturnListener() {
                @Override
                public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println("return message: " + new String(body, "UTF-8"));
                    System.out.println("replyCode: " + replyCode);
                    System.out.println("replyText: " + replyText);
                    System.out.println("exchange: " + exchange);
                    System.out.println("routingKey: " + routingKey);
                }
            });

            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .deliveryMode(2).
                    contentEncoding("UTF-8")
                    .build();

            // 备份交换机
            channel.exchangeDeclare("ALTERNATE_EXCHANGE","topic", false, false, false, null);
            channel.queueDeclare("ALTERNATE_QUEUE", false, false, false, null);
            channel.queueBind("ALTERNATE_QUEUE","ALTERNATE_EXCHANGE","#");

            // 在声明交换机的时候指定备份交换机
            Map<String,Object> arguments = new HashMap<String,Object>();
            arguments.put("alternate-exchange","ALTERNATE_EXCHANGE");


            //声明队列
//            channel.queueDeclare(DEMO_RETURN_QUEUE, true, false, false, null);
            channel.exchangeDeclare(DEMO_RETURN_EXCHANGE, "topic", false, false, false, arguments);

            //发送消息
            String msg = "hello ack message";

            channel.basicPublish(DEMO_RETURN_EXCHANGE, "demo",true, properties, msg.getBytes());

            TimeUnit.SECONDS.sleep(5);
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
            e.printStackTrace();
        } finally {
//            try {
//                channel.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (TimeoutException e) {
//                e.printStackTrace();
//            }
//            try {
//                connection.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }
    }
}
