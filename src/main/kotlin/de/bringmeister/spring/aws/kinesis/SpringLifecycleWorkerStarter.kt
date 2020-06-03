package de.bringmeister.spring.aws.kinesis

import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import software.amazon.kinesis.coordinator.Scheduler
import java.time.Duration
import java.util.WeakHashMap
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean

class SpringLifecycleWorkerStarter(
    delayWorkerStartUntilComponentStarted: Boolean = true,
    private val threadFactory: ThreadFactory = ThreadFactory { Thread(it) },
    private val workerShutdownTimeout: Duration = Duration.ofSeconds(5),
    private val workerShutdownMaxNumberThreads: Int = 4
) : WorkerStarter, SmartLifecycle {

    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        /** We're last to start and first to shutdown. */
        const val KINESIS_PHASE = Integer.MAX_VALUE
    }

    private val started = AtomicBoolean(!delayWorkerStartUntilComponentStarted)
    private val delayedStart = mutableMapOf<String, Scheduler>()

    /**
     * Only keep workers that are referenced elsewhere.
     * Workers that shut down externally are automatically removed.
     */
    private val workers = WeakHashMap<Scheduler, String>()

    override fun getPhase(): Int = KINESIS_PHASE

    override fun isRunning(): Boolean {
        return workers.keys.any { !it.hasGracefulShutdownStarted() }
    }

    @Synchronized
    override fun start() {
        log.info("Kinesis worker threads start initiated...")

        for ((stream, worker) in delayedStart) {
            startWorkerNow(stream, worker)
        }

        started.set(true)
        log.info("Kinesis worker threads started.")
    }

    override fun stop() {
        // Runs in parallel to all other hooks with the same phase value,
        // but before everyone with lower values.

        log.info("Kinesis worker shutdown initiated...")

        // stop kinesis workers and await their shutdown
        try {
            val shutdownTasks = workers.map { (worker, stream) ->
                Callable {
                    log.info("Shutting down Kinesis worker of stream <{}>...", stream)
                    if (workerShutdownTimeout.isZero) {
                        worker.shutdown()
                        log.info("Lease Coordinator of stream <{}> was shut down. Workers will follow.", stream)
                    } else {
                        val future = worker.startGracefulShutdown()
                        try {
                            when (future.get(workerShutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                                true -> log.info(
                                    "Kinesis worker of stream <{}> successfully shut down in {}.",
                                    stream, Duration.ofMillis(System.currentTimeMillis() - worker.shutdownStartTimeMillis()))
                                false -> {
                                    // KCL 2.x runs quite regularly into a race condition reporting incomplete shutdown.
                                    // Therefore, we're additionally waiting for the scheduler to successfully report
                                    // shutdown as per the workaround mentioned in #542.
                                    // @see https://github.com/awslabs/amazon-kinesis-client/issues/542
                                    // @see https://github.com/awslabs/amazon-kinesis-client/issues/616
                                    while (!worker.shutdownComplete()) {
                                        log.info("Waiting for Scheduler of stream <{}> to shut down...", stream)
                                        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(500))
                                    }
                                    log.info(
                                        "Scheduler of stream <{}> shut down successfully in {}.",
                                        stream, Duration.ofMillis(System.currentTimeMillis() - worker.shutdownStartTimeMillis()))
                                }
                            }
                        } catch (ex: TimeoutException) {
                            log.error("Kinesis worker of stream <{}> did not properly shut down within {} seconds.", stream, workerShutdownTimeout.seconds)
                        } catch (ex: ExecutionException) {
                            log.error("Kinesis worker of stream <{}> shut down with an exception.", stream, ex)
                        }
                    }
                }
            }

            if (shutdownTasks.isNotEmpty()) {
                val executor = Executors.newFixedThreadPool(workerShutdownMaxNumberThreads.coerceAtMost(shutdownTasks.size))
                executor.invokeAll(shutdownTasks)
                executor.shutdown()
                executor.awaitTermination(workerShutdownTimeout.toMillis() * workers.size, TimeUnit.MILLISECONDS)
            }
        } catch (ex: Exception) {
            log.error("Shutdown of Kinesis workers failed.", ex)
        }

        log.info("Kinesis worker shutdown phase completed.")
    }

    private fun startWorkerDelayed(stream: String, worker: Scheduler) {
        log.debug("Start of Kinesis worker for stream <{}> is delayed until Spring application is fully loaded.", stream)
        delayedStart[stream] = worker
    }

    private fun startWorkerNow(stream: String, worker: Scheduler) {
        log.debug("Starting Kinesis worker for stream <{}>...", stream)
        threadFactory.newThread(worker)
            .apply { name = "worker-$stream" }
            .start()
        log.info("Kinesis worker for stream <{}> started.", stream)
        workers[worker] = stream
    }

    @Synchronized
    override fun startWorker(stream: String, worker: Scheduler) {
        when (started.get()) {
            true -> startWorkerNow(stream, worker)
            false -> startWorkerDelayed(stream, worker)
        }
    }
}
