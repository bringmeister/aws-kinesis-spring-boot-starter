package de.bringmeister.spring.aws.kinesis

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class KinesisListenerProxyFactoryTest {

    var kinesisListenerProxyFactory: KinesisListenerProxyFactory = KinesisListenerProxyFactory(AopProxyUtils())

    @Test
    fun `should return empty list if no Kinesis listeners are present`() {
        val kinesisListenerProxies = kinesisListenerProxyFactory.proxiesFor(Object())

        assertThat(kinesisListenerProxies).isEmpty()
    }

    @Test
    fun `should return handlers for annotated methods`() {
        val dummyListener = DummyListener()
        val kinesisListenerProxies = kinesisListenerProxyFactory.proxiesFor(dummyListener)

        assertThat(kinesisListenerProxies).hasSize(2)
        assertThat(kinesisListenerProxies.stream().map { it.stream }).contains("stream-1", "stream-2")
    }

    private class DummyListener {

        @KinesisListener(stream = "stream-1")
        fun listener1(data: FooCreatedEvent, metadata: EventMetadata) {
            // empty
        }

        @KinesisListener(stream = "stream-2")
        fun listener2(data: FooCreatedEvent, metadata: EventMetadata) {
            // empty
        }
    }
}
