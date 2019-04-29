package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
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

    private val started = AtomicBoolean(!delayWorkerStartUntilComponentStarted)
    private val delayedStart = mutableMapOf<String, Worker>()

    /**
     * Only keep workers that are referenced elsewhere.
     * Workers that shut down externally are automatically removed.
     */
    private val workers = WeakHashMap<Worker, String>()

    /** We're last to start and first to shutdown. */
    override fun getPhase(): Int = Integer.MAX_VALUE

    override fun isRunning(): Boolean {
        return workers.keys.any { !it.hasGracefulShutdownStarted() }
    }

    @Synchronized
    override fun start() {
        log.info("Kinesis Worker threads start initiated...")

        for ((stream, worker) in delayedStart) {
            startWorkerNow(stream, worker)
        }

        started.set(true)
        log.info("Kinesis Worker threads started.")
    }

    override fun stop() {
        // Runs in parallel to all other hooks with the same phase value,
        // but before everyone with lower values.

        log.info("Kinesis Worker shutdown initiated...")

        // stop kinesis workers and await their shutdown
        try {
            val shutdownTasks = workers.map { (worker, stream) ->
                Callable {
                    log.info("Shutting down Kinesis worker of stream <{}>...", stream)
                    val future = worker.startGracefulShutdown()
                    try {
                        when (future.get(workerShutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                            true -> log.info("Kinesis worker of stream <{}> successfully shut down.", stream)
                            false -> log.error(
                                "Kinesis worker of stream <{}> was shut down with unexpected status. " +
                                "Check preceding log statements from AWS Kinesis Library for additional information.",
                                stream
                            )
                        }
                    } catch (ex: TimeoutException) {
                        log.error("Kinesis worker of stream <$stream> did not properly shut down within ${workerShutdownTimeout.seconds} seconds.")
                    } catch (ex: ExecutionException) {
                        log.error("Kinesis worker of stream <$stream> shut down with an exception.", ex)
                    }
                }
            }

            if (shutdownTasks.isNotEmpty()) {
                val executor = Executors.newFixedThreadPool(Math.min(workerShutdownMaxNumberThreads, shutdownTasks.size))
                executor.invokeAll(shutdownTasks)
                executor.shutdown()
                executor.awaitTermination(workerShutdownTimeout.toMillis() * workers.size, TimeUnit.MILLISECONDS)
            }
        } catch (ex: Exception) {
            log.error("Shutdown of Kinesis workers failed.", ex)
        }

        log.info("Kinesis Worker shutdown phase completed.")
    }

    private fun startWorkerDelayed(stream: String, worker: Worker) {
        delayedStart[stream] = worker
    }

    private fun startWorkerNow(stream: String, worker: Worker) {
        threadFactory.newThread(worker)
            .apply { name = "worker-$stream" }
            .start()
        workers[worker] = stream
    }

    @Synchronized
    override fun startWorker(stream: String, worker: Worker) {
        when (started.get()) {
            true -> startWorkerNow(stream, worker)
            false -> startWorkerDelayed(stream, worker)
        }
    }
}
