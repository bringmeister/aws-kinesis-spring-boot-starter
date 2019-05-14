package de.bringmeister.spring.aws.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JavaTestListener {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @KinesisListener(stream = "foo-event-stream")
    public void handle(FooCreatedEvent data, EventMetadata metadata) {
        log.info("Java Kinesis listener caught message");
        JavaListenerTest.LATCH.countDown();
    }

    @KinesisListener(stream = "foo-event-stream-batch")
    public void handleBatch(Map<FooCreatedEvent, EventMetadata> events) {
        log.info("Java Kinesis listener caught message");
        JavaListenerTest.LATCH.countDown();
    }
}
