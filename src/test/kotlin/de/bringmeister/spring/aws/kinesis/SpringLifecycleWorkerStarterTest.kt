package de.bringmeister.spring.aws.kinesis

import com.nhaarman.mockito_kotlin.doAnswer
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.Test
import org.springframework.scheduling.concurrent.CustomizableThreadFactory
import software.amazon.kinesis.coordinator.Scheduler
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class SpringLifecycleWorkerStarterTest {

    @Test
    fun `should start the given runnable`() {
        val executed = AtomicBoolean(false)
        val worker = mockWorkerRunnable {
            assertThat(Thread.currentThread().name).isEqualTo("worker-test-stream")
            executed.set(true)
        }
        val workerStarter = SpringLifecycleWorkerStarter(delayWorkerStartUntilComponentStarted = false)

        workerStarter.startWorker("test-stream", worker)

        await()
            .atMost(2, TimeUnit.SECONDS)
            .untilAsserted { assertThat(executed).isTrue }
    }

    @Test
    fun `should use the configured ThreadFactory`() {
        val executed = AtomicBoolean(false)
        val worker = mockWorkerRunnable {
            assertThat(Thread.currentThread().threadGroup.name).isEqualTo("custom-group")
            executed.set(true)
        }
        val workerStarter = SpringLifecycleWorkerStarter(
            delayWorkerStartUntilComponentStarted = false,
            threadFactory = CustomizableThreadFactory().apply { setThreadGroupName("custom-group") }
        )

        workerStarter.startWorker("test-stream", worker)

        await()
            .atMost(2, TimeUnit.SECONDS)
            .untilAsserted { assertThat(executed).isTrue }
    }

    @Test
    fun `should delay start of workers until triggered by Spring`() {
        val executed = AtomicBoolean(false)
        val worker = mockWorkerRunnable { executed.set(true) }
        val workerStarter = SpringLifecycleWorkerStarter()

        workerStarter.startWorker("test-stream", worker)
        await()
            .atLeast(50, TimeUnit.MILLISECONDS)
            .untilAsserted { assertThat(executed).isFalse }

        workerStarter.start()
        await()
            .atMost(2, TimeUnit.SECONDS)
            .untilAsserted { assertThat(executed).isTrue }
    }

    @Test
    fun `should stop workers when triggered by Spring`() {
        val workerShutdownLatch = CountDownLatch(1)
        val worker = mockWorkerRunnable { workerShutdownLatch.await() }
        whenever(worker.startGracefulShutdown()).thenAnswer {
            workerShutdownLatch.countDown()
            CompletableFuture.completedFuture(true)
        }
        val thread = Thread(worker)
        val workerStarter = SpringLifecycleWorkerStarter(
            delayWorkerStartUntilComponentStarted = false,
            threadFactory = ThreadFactory { thread },
            workerShutdownTimeout = Duration.ofSeconds(4)
        )

        workerStarter.startWorker("test-stream", worker)
        await()
            .atMost(2, TimeUnit.SECONDS)
            .untilAsserted { assertThat(thread.isAlive).isTrue() }

        workerStarter.stop()
        await()
            .atMost(2, TimeUnit.SECONDS)
            .untilAsserted { assertThat(thread.isAlive).isFalse() }
    }

    private fun mockWorkerRunnable(lambda: () -> Unit): Scheduler {
        return mock {
            on { run() } doAnswer { lambda.invoke() }
        }
    }
}
