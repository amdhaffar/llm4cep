package com.example.cep.config;

/**
 * Simple configuration holder for Kafka topics and brokers.
 */
public final class KafkaConfig {

    private KafkaConfig() {
    }

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String MEDICAL_TOPIC = "medical-events";
    public static final String ENVIRONMENTAL_TOPIC = "environmental-events";
    public static final String RECOMMENDATION_TOPIC = "recommendations";
}
