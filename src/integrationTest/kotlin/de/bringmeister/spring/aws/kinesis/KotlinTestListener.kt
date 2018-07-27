package de.bringmeister.spring.aws.kinesis

import org.slf4j.LoggerFactory

class KotlinTestListener {
    private val log = LoggerFactory.getLogger(this.javaClass.name)

    @KinesisListener(stream = "foo-event-stream")
    fun handle(data: FooCreatedEvent, metadata: EventMetadata) {
        log.info("Kotlin Kinesis listener caught message")
        KotlinListenerTest.latch.countDown()
    }
}