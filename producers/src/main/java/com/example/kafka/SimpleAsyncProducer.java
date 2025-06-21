package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleAsyncProducer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleAsyncProducer.class.getName());

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
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception != null) {
                    logger.error("exception error from broker", exception);
                    return;
                }

                logger.info("metadata.timestamp()  = {}", metadata.timestamp());
                logger.info("metadata.partitions() = {}", metadata.partition());
                logger.info("metadata.offset()     = {}", metadata.offset());
            }
        });

        // wait for callback
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
