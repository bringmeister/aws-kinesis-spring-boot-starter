package de.bringmeister.spring.aws.kinesis

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Scope
import org.springframework.context.annotation.ScopedProxyMode
import org.springframework.stereotype.Service
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringRunner

@SpringBootTest
@RunWith(SpringRunner::class)
@ContextConfiguration(classes = [KinesisListenerProxyFactoryAopTest.DummyListener::class])
class KinesisListenerProxyFactoryAopTest {

    var kinesisListenerProxyFactory: KinesisListenerProxyFactory = KinesisListenerProxyFactory(AopProxyUtils())

    @Autowired
    lateinit var dummyListener: DummyListener

    @Test
    fun `should return list of Kinesis listeners`() {

        val kinesisListenerProxies = kinesisListenerProxyFactory.proxiesFor(dummyListener)

        assertThat(kinesisListenerProxies).hasSize(2)
        assertThat(kinesisListenerProxies.map { it.stream }).contains("stream-1", "stream-2")
    }

    // This class will be autowired wrapped in a proxy. This proxy is created
    // by Spring. We want to be able to register the methods anyway.
    @Service
    @Scope("singleton", proxyMode = ScopedProxyMode.TARGET_CLASS)
    class DummyListener {

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
