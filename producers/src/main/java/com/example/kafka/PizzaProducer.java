package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducer {
    private static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class.getName());

    public static void main(String[] args) {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(getProperties());
        sendPizzaMessage(
                kafkaProducer,
                "pizza-topic",
                -1,
                100,
                1000,
                100,
                true
        );

        kafkaProducer.close();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.64.143:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        ack setting
//        props.setProperty(ProducerConfig.ACKS_CONFIG, "0");

//        batch setting
//        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32000");
//        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        return props;
    }

    private static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName,
                                        int iterationCount,
                                         int interIntervalMillis,
                                        int intervalMillis,
                                        int intervalCount,
                                        boolean sync) {
        PizzaMessage pizzaMessage = new PizzaMessage(new Random());

        int i = 0;
        while(i++ != iterationCount) {
            Map<String, String> message = pizzaMessage.produceMsg(i);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                    topicName,
                    message.get("key"),
                    message.get("message")
            );

            sendMessage(
                    kafkaProducer,
                    producerRecord,
                    message,
                    sync
            );

            if((intervalCount > 0) && (i % intervalCount ==0)) {
                try {
                    logger.info("intervalCount={}, intervalMillis={}", intervalCount, intervalMillis);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if(interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMilis={}", interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   Map<String, String> message,
                                   boolean sync) {
        if(!sync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("exception error from broker", exception);
                    return;
                }

                logMessage("async message={}, partition={}, offset={}", message, metadata);
            });

            return;
        }

        try {
            RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
            logMessage("sync message={}, partition={}, offset={}", message, metadata);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void logMessage(String format, Map<String, String> message, RecordMetadata metadata) {
        logger.info(format,
                message.get("key"),
                metadata.partition(),
                metadata.offset()
        );
    }
}
