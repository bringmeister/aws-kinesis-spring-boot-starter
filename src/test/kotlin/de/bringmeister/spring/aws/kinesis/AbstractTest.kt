package de.bringmeister.spring.aws.kinesis

import com.fasterxml.jackson.databind.ObjectMapper
import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.local.KinesisLocalConfiguration
import org.junit.runner.RunWith
import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import java.time.OffsetDateTime

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [AbstractTest.ApplicationConfiguration::class])
@ActiveProfiles("test")
abstract class AbstractTest {

    @Configuration
    @ImportAutoConfiguration(AwsKinesisAutoConfiguration::class, KinesisLocalConfiguration::class)
    class ApplicationConfiguration {

        @Bean
        fun objectMapper() = mock<ObjectMapper> { }
    }
}

data class Foo(val name: String)
data class FooCreatedEvent(val foo: Foo)
data class EventMetadata(val occurredAt: OffsetDateTime?, val traceId: String?)
data class FooCreatedKinesisEvent(val metadata: EventMetadata, val data: FooCreatedEvent) : KinesisEvent<FooCreatedEvent, EventMetadata> {
    companion object {
        const val STREAM_NAME = "foo-event-stream"
    }

    override fun data() = data
    override fun metadata() = metadata
    override fun streamName() = STREAM_NAME
}
