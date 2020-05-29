package de.bringmeister.spring.aws.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaTestListener {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @KinesisListener(stream = "foo-event-stream")
    public void handle(FooCreatedEvent data, EventMetadata metadata) {
        log.info("Java Kinesis listener caught message");
        JavaListenerTest.LATCH.countDown();
    }
}
