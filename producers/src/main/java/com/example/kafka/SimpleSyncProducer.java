package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleSyncProducer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleSyncProducer.class.getName());

    public static void main(String[] args) {
        // set KafkaProducer configuration
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.64.143:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create KafkaProducer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // create ProducerRecord
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                "simple-topic",
                "id-001",
                "hello world"
        );

        // send message
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("recordMetadata.partition() = {}", recordMetadata.partition());
            logger.info("recordMetadata.offsets()   = {}", recordMetadata.offset());
            logger.info("recordMetadata.timestamp() = {}", recordMetadata.timestamp());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            // close
            kafkaProducer.close();
        }
    }
}
