package de.bringmeister.spring.aws.kinesis.health

import de.bringmeister.spring.aws.kinesis.WorkerInitializedEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

@Component
class KinesisListenerRegistry {

    private val log: Logger = LoggerFactory.getLogger(this.javaClass)
    private var latch: CountDownLatch? = null
    private var streamCount = AtomicInteger(0)
    private val streams = mutableMapOf<String, Boolean>()

    @EventListener
    fun workerInitializedEvent(workerInitializedEvent: WorkerInitializedEvent) {
        if (latch == null) {
            latch = CountDownLatch(streamCount.get())
        }
        val stream = workerInitializedEvent.streamName
        if (streams[stream] == false) {
            latch!!.countDown()
            streams[stream] = true
            log.info("Kinesis listener is initialized. [stream={}]", stream)
        }
    }

    fun areAllListenersInitialized(): Boolean {
        if (latch == null) {
            latch = CountDownLatch(streamCount.get())
        }
        return latch!!.count <= 0
    }

    fun addStreamToCheckFor(stream: String) {
        if (!streams.containsKey(stream)) {
            log.info("Wait for Kinesis listener to be initialized. [stream={}]", stream)
            streams[stream] = false
            streamCount.incrementAndGet()
        }
    }

    fun initializedKinesisListeners() = streams.keys
}
