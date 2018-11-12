package de.bringmeister.spring.aws.kinesis

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.util.concurrent.CountDownLatch

class WorkerStarterTest {

    @Test
    fun `should start the given runnable`() {
        val latch = CountDownLatch(1)
        val runnable = Runnable { latch.countDown() }
        val workerStarter = WorkerStarter()
        workerStarter.start(runnable)
        latch.await() // wait for event-listener thread to process event
        assertThat(latch.count).isEqualTo(0L)
    }
}