package de.bringmeister.spring.aws.kinesis

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.util.concurrent.CountDownLatch

class KinesisListenerProxyTest {

    private val latchOfThread: CountDownLatch = CountDownLatch(1)
    private val latchOfTest: CountDownLatch = CountDownLatch(1)
    private val handler = object {
        @KinesisListener(stream = "foo-event-stream")
        fun handle(data: FooCreatedEvent, metadata: EventMetadata) {
            latchOfThread.await()
            latchOfTest.countDown()
        }
    }

    @Test
    fun `should run invocation in separate thread`() {
        val proxy = KinesisListenerProxyFactory().proxiesFor(handler)[0]
        proxy.invoke(FooCreatedEvent("any"), EventMetadata("any"))
        latchOfThread.countDown()
        latchOfTest.await()

        assertThat(latchOfThread.count).isEqualTo(0)
        assertThat(latchOfTest.count).isEqualTo(0)
    }
}