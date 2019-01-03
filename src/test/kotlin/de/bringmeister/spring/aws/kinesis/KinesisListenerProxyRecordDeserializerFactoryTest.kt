package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.model.Record
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.nio.ByteBuffer

class KinesisListenerProxyRecordDeserializerFactoryTest {

    val messageJson = """{"data":{"foo":"any-field"},"metadata":{"sender":"test"}}"""
    val mapper = ObjectMapper().registerModule(KotlinModule())

    @Test
    fun `should deserialize record with a listener implementing the interface`() {

        val handler = object {
            @KinesisListener(stream = "foo-event-stream")
            fun handle(data: FooCreatedEvent, metadata: EventMetadata) { /* nothing to do */
            }
        }

        val kinesisListenerProxy = KinesisListenerProxyFactory(AopProxyUtils()).proxiesFor(handler)[0]

        val deserializer = KinesisListenerProxyRecordDeserializerFactory(mapper)
            .deserializerFor(kinesisListenerProxy)
        val awsRecord = Record()
            .withData(ByteBuffer.wrap(messageJson.toByteArray(Charsets.UTF_8)))
        val message = deserializer.deserialize(awsRecord)

        assertThat(message.data).isEqualTo(FooCreatedEvent("any-field"))
        assertThat(message.metadata).isEqualTo(EventMetadata("test"))
    }
}
