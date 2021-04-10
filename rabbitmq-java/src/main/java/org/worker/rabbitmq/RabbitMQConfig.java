package org.worker.rabbitmq;

public class RabbitMQConfig {
    private RabbitMQConfig(){}
    public static final String RABBITMQ_URI = "amqp://guest:guest@127.0.0.1:5672";
    public static final String RABBITMQ_ROUTING_TYPE_DIRECT = "direct";
    public static final String RABBITMQ_ROUTING_TYPE_TOPIC = "topic";
    public static final String RABBITMQ_ROUTING_TYPE_FANOUT = "fanout";
    public static final String RABBITMQ_ROUTING_TYPE_HEADER = "header";
}
