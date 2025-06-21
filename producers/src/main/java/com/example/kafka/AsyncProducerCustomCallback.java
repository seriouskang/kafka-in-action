package com.example.kafka;

import com.example.kafka.callback.CustomCallback;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AsyncProducerCustomCallback {
    private static final Logger logger = LoggerFactory.getLogger(AsyncProducerCustomCallback.class.getName());

    public static void main(String[] args) {
        // set KafkaProducer configuration
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.64.143:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create KafkaProducer
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        // create ProducerRecord
        for(int i=0; i<20; i++) {
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(
                    "multi-topic",
                    i,
                    "hello world " + i
            );

            // send message
            kafkaProducer.send(producerRecord, new CustomCallback(i));
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
