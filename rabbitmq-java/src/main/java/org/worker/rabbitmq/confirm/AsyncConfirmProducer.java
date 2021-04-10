package org.worker.rabbitmq.confirm;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.worker.rabbitmq.RabbitMQConfig;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

public class AsyncConfirmProducer {
    private static final String QUEUE_CONFIRM_NAME = "DEMO_CONFIRM_QUEUE";
    private static final String EXCHANGE_CONFIRM_NAME = "DEMO_CONFIRM_EXCHANGE";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(RabbitMQConfig.RABBITMQ_URI);
            //建立连接
            Connection connection = factory.newConnection();
            //创建信道
            Channel channel = connection.createChannel();

            //声明队列
            channel.queueDeclare(QUEUE_CONFIRM_NAME, true, false, false, null);

            //未确认消息的deliveryTag
            final SortedSet<Long> confirmSet = Collections.synchronizedSortedSet(new TreeSet<Long>());

            //监听ACK
            channel.addConfirmListener(new ConfirmListener() {
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    // 如果true表示批量执行了deliveryTag这个值以前（小于deliveryTag的）的所有消息，如果为false的话表示单条确认
                    System.out.println("Broker已确认消息的标识：" + deliveryTag);
                    if(multiple) {
                        //headSet表示后面参数之前的所有元素，全部删除
                        confirmSet.headSet(deliveryTag + 1L).clear();
                    }else {
                        confirmSet.remove(deliveryTag);
                    }

                }

                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    System.out.println("Broker未确认消息的标识：" + deliveryTag);
                    if(multiple) {
                        //headSet表示后面参数之前的所有元素，全部删除
                        confirmSet.headSet(deliveryTag + 1L).clear();
                    }else {
                        confirmSet.remove(deliveryTag);
                    }
                }
            });
            //开启确认模式
            channel.confirmSelect();
            String msg = "hello async confirm";
            for(int i = 0; i < 10; i++) {
                long nextPublishSeqNo = channel.getNextPublishSeqNo();
                channel.basicPublish(EXCHANGE_CONFIRM_NAME, "demo.confirm", null, (msg + "-" + nextPublishSeqNo).getBytes());
                confirmSet.add(nextPublishSeqNo);
            }


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
