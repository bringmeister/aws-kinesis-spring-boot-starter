package de.bringmeister.spring.aws.kinesis

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class ThreadFactoryWorkerStarterTest {

    @Test
    fun `should start the given runnable`() {
        val latch = CountDownLatch(1)
        val runnable = Runnable {
            assertThat(Thread.currentThread().name).isEqualTo("worker-test-stream")
            latch.countDown()
        }
        val workerStarter = ThreadFactoryWorkerStarter()
        workerStarter.start("test-stream", runnable)
        latch.await(2, TimeUnit.SECONDS) // wait for event-listener thread to process event
        assertThat(latch.count).isEqualTo(0L)
    }

    @Test
    fun `should use the configured ThreadFactory`() {
        val latch = CountDownLatch(1)
        val runnable = Runnable {
            assertThat(Thread.currentThread().threadGroup.name).isEqualTo("custom-group")
            latch.countDown()
        }

        val workerStarter = ThreadFactoryWorkerStarter(CustomizableThreadFactory().apply { setThreadGroupName("custom-group") })
        workerStarter.start("test-stream", runnable)
        latch.await(2, TimeUnit.SECONDS) // wait for event-listener thread to process event
        assertThat(latch.count).isEqualTo(0L)
    }
}
