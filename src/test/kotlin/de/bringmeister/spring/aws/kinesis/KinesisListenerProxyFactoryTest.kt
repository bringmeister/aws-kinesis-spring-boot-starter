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
    fun `should return list no Kinesis listeners`() {
        val dummyListener = DummyListener()
        val kinesisListenerProxies = kinesisListenerProxyFactory.proxiesFor(dummyListener).map { it.stream to it}.toMap()


        assertThat(kinesisListenerProxies).hasSize(4)
        assertThat(kinesisListenerProxies.keys).contains("stream-1", "stream-2", "stream-3", "stream-4")
        assertThat(kinesisListenerProxies["stream-1"]?.mode).isEqualTo(ListenerMode.DATA_METADATA)
        assertThat(kinesisListenerProxies["stream-4"]?.mode).isEqualTo(ListenerMode.RECORD)
        assertThat(kinesisListenerProxies["stream-3"]?.mode).isEqualTo(ListenerMode.BATCH)
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

        @KinesisListener(stream = "stream-3")
        fun listener3(record: List<Record<FooCreatedEvent, EventMetadata>>) {
            // empty
        }

        @KinesisListener(stream = "stream-4")
        fun listener4(record: Record<FooCreatedEvent, EventMetadata>) {
            // empty
        }
    }
}
