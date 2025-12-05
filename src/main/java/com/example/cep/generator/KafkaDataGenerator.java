package com.example.cep.generator;

import com.example.cep.config.KafkaConfig;
import com.example.cep.model.EnvironmentalEvent;
import com.example.cep.model.MedicalEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

/**
 * Kafka producer that continuously generates pseudo realistic events.
 */
public class KafkaDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDataGenerator.class);

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                MedicalEvent medical = randomMedicalEvent(random);
                EnvironmentalEvent env = randomEnvironmentalEvent(random, medical.getLocationId());

                producer.send(new ProducerRecord<>(KafkaConfig.MEDICAL_TOPIC, medical.getPatientId(),
                        mapper.writeValueAsString(medical)));
                producer.send(new ProducerRecord<>(KafkaConfig.ENVIRONMENTAL_TOPIC, env.getLocationId(),
                        mapper.writeValueAsString(env)));

                LOG.info("Published medical event: {}", medical);
                LOG.info("Published environmental event: {}", env);

                Thread.sleep(1000L);
            }
        }
    }

    private static MedicalEvent randomMedicalEvent(Random random) {
        String patientId = "patient-" + (1 + random.nextInt(3));
        String locationId = "location-" + (1 + random.nextInt(2));
        long timestamp = Instant.now().toEpochMilli();

        // Normal ranges with occasional spikes
        double heartRate = 70 + random.nextGaussian() * 10;
        if (random.nextDouble() < 0.15) {
            heartRate = 130 + random.nextGaussian() * 10; // tachycardia episode
        }
        double systolic = 120 + random.nextGaussian() * 15;
        if (random.nextDouble() < 0.1) {
            systolic = 170 + random.nextGaussian() * 10; // hypertension episode
        }
        double diastolic = 80 + random.nextGaussian() * 10;
        double spo2 = 97 + random.nextGaussian() * 1.5;
        if (random.nextDouble() < 0.12) {
            spo2 = 90 + random.nextGaussian(); // oxygen drop
        }

        return new MedicalEvent(patientId, locationId, timestamp, heartRate, systolic, diastolic, spo2);
    }

    private static EnvironmentalEvent randomEnvironmentalEvent(Random random, String locationId) {
        long timestamp = Instant.now().toEpochMilli();
        double aqi = 60 + random.nextGaussian() * 15;
        if (random.nextDouble() < 0.12) {
            aqi = 180 + random.nextGaussian() * 20; // pollution spike
        }
        double temperature = 15 + random.nextGaussian() * 10;
        double humidity = 50 + random.nextGaussian() * 20;
        double pm25 = 12 + random.nextGaussian() * 5;
        if (aqi > 150) {
            pm25 = 80 + random.nextGaussian() * 10;
        }
        return new EnvironmentalEvent(locationId, timestamp, aqi, temperature, humidity, pm25);
    }
}
