package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AsyncProducerWithKey {
    private static final Logger logger = LoggerFactory.getLogger(AsyncProducerWithKey.class.getName());

    public static void main(String[] args) {
        // set KafkaProducer configuration
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.64.143:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create KafkaProducer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // create ProducerRecord
        for(int i=0; i<20; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    "multi-topic",
                    String.valueOf(i),
                    "hello world " + i
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
        }

        // wait for callback
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.close();
    }
}
