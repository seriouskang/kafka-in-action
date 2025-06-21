package com.example.kafka.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {
    private static final Logger logger = LoggerFactory.getLogger(CustomCallback.class.getName());
    private final int seq;

    public CustomCallback(int seq) {
        this.seq = seq;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception != null) {
            logger.error("exception error from broker", exception);
            return;
        }

        logger.info("timestamp = {}, seq = {}, partitions = {}, offset = {}",
                metadata.timestamp(),
                seq,
                metadata.partition(),
                metadata.offset()
        );
    }
}
