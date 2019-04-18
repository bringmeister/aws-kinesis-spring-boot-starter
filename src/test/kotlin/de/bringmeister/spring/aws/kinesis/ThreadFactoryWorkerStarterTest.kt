package de.bringmeister.spring.aws.kinesis

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit

class ThreadFactoryWorkerStarterTest {

    @Test
    fun `should start the given runnable`() {
        val latch = CountDownLatch(1)
        val runnable = Runnable { latch.countDown() }
        val workerStarter = ThreadFactoryWorkerStarter()
        workerStarter.start(runnable)
        latch.await(2, TimeUnit.SECONDS) // wait for event-listener thread to process event
        assertThat(latch.count).isEqualTo(0L)
    }

    @Test
    fun `should use the configured ThreadFactory`() {
        val latch = CountDownLatch(1)
        val runnable = Runnable {
            assertThat(Thread.currentThread().name).isEqualTo("dedicated-test-thread")
            latch.countDown()
        }
        val threadFactory = ThreadFactory { Thread(it).apply { name = "dedicated-test-thread" } }
        val workerStarter = ThreadFactoryWorkerStarter(threadFactory)
        workerStarter.start(runnable)
        latch.await(2, TimeUnit.SECONDS) // wait for event-listener thread to process event
        assertThat(latch.count).isEqualTo(0L)
    }
}
