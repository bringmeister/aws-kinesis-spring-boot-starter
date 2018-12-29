package de.bringmeister.spring.aws.kinesis

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class ReflectionBasedRecordMapperTest {

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

        val recordMapper = ReflectionBasedRecordMapper(mapper)
        val message = recordMapper.deserializeFor(messageJson, kinesisListenerProxy)

        assertThat(message.data).isEqualTo(FooCreatedEvent("any-field"))
        assertThat(message.metadata).isEqualTo(EventMetadata("test"))
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should throw IllegalArgumentException for unsupported handler types`() {
        val mapper = ReflectionBasedRecordMapper(mapper)
        mapper.deserializeFor("any", TestKinesisInboundHandler())
    }
}
