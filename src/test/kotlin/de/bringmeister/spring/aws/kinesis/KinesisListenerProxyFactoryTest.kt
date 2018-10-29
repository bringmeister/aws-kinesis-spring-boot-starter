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
        val kinesisListenerProxies = kinesisListenerProxyFactory.proxiesFor(dummyListener).map { it.stream to it }.toMap()


        assertThat(kinesisListenerProxies).hasSize(3)
        assertThat(kinesisListenerProxies.keys).contains("stream-data-metadata", "stream-batch", "stream-record")
        assertThat(kinesisListenerProxies["stream-data-metadata"]?.mode).isEqualTo(ListenerMode.DATA_METADATA)
        assertThat(kinesisListenerProxies["stream-batch"]?.mode).isEqualTo(ListenerMode.BATCH)
        assertThat(kinesisListenerProxies["stream-record"]?.mode).isEqualTo(ListenerMode.RECORD)
    }

    @Test
    fun `should assign BATCH listener mode`() {
        val dummyListener = DummyListener()
        val kinesisListenerProxies = kinesisListenerProxyFactory.proxiesFor(dummyListener).map { it.stream to it }.toMap()

        assertThat(kinesisListenerProxies.containsKey("stream-batch"))
        assertThat(kinesisListenerProxies["stream-batch"]?.mode).isEqualTo(ListenerMode.BATCH)
    }

    @Test
    fun `should assign RECORD listener mode`() {
        val dummyListener = DummyListener()
        val kinesisListenerProxies = kinesisListenerProxyFactory.proxiesFor(dummyListener).map { it.stream to it }.toMap()

        assertThat(kinesisListenerProxies.containsKey("stream-record"))
        assertThat(kinesisListenerProxies["stream-record"]?.mode).isEqualTo(ListenerMode.RECORD)
    }

    @Test
    fun `should assign DATA_METADATA listener mode`() {
        val dummyListener = DummyListener()
        val kinesisListenerProxies = kinesisListenerProxyFactory.proxiesFor(dummyListener).map { it.stream to it }.toMap()

        assertThat(kinesisListenerProxies.containsKey("stream-data-metadata"))
        assertThat(kinesisListenerProxies["stream-data-metadata"]?.mode).isEqualTo(ListenerMode.DATA_METADATA)
    }

    private class DummyListener {

        @KinesisListener(stream = "stream-data-metadata")
        fun listenerDM(data: FooCreatedEvent, metadata: EventMetadata) {
            // empty
        }

        @KinesisListener(stream = "stream-batch")
        fun listenerBatch(record: List<Record<FooCreatedEvent, EventMetadata>>) {
            // empty
        }

        @KinesisListener(stream = "stream-record")
        fun listenerRecord(record: Record<FooCreatedEvent, EventMetadata>) {
            // empty
        }
    }
}
