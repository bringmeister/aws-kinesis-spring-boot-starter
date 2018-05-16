package de.bringmeister.spring.aws.kinesis

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class KinesisListenerProxyFactoryTest {

    var kinesisListenerProxyFactory : KinesisListenerProxyFactory = KinesisListenerProxyFactory()

    @Test
    fun `should return empty list if no Kinesis listeners are present`() {
        val kinesisListenerProxies = kinesisListenerProxyFactory.proxiesFor(Object())

        assertThat(kinesisListenerProxies).isEmpty()
    }

    @Test
    fun `should return list no Kinesis listeners`() {
        val dummyListener = DummyListener()
        val kinesisListenerProxies = kinesisListenerProxyFactory
                                                .proxiesFor(dummyListener)
                                                .sortedWith(compareBy({ it.stream })) // sort it for constant order!

        assertThat(kinesisListenerProxies).hasSize(2)

        assertThat(kinesisListenerProxies[0].stream).isEqualTo("stream-1")
        assertThat(kinesisListenerProxies[0].bean).isEqualTo(dummyListener)
        assertThat(kinesisListenerProxies[0].method).isNotNull

        assertThat(kinesisListenerProxies[1].stream).isEqualTo("stream-2")
        assertThat(kinesisListenerProxies[1].bean).isEqualTo(dummyListener)
        assertThat(kinesisListenerProxies[1].method).isNotNull
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
