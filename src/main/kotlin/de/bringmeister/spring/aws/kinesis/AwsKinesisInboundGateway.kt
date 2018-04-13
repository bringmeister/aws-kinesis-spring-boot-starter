package de.bringmeister.spring.aws.kinesis

import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory

class AwsKinesisInboundGateway(val objectMapper: ObjectMapper,
                               private val clientProvider: AwsKinesisClientProvider,
                               private val workerFactory: WorkerFactory) {

    private val log = LoggerFactory.getLogger(this.javaClass)

    fun <D, M, DClass : Class<D>, MClass : Class<M>> listen(streamName: String,
                                                            eventHandler: (D, M) -> Unit,
                                                            dataClass: DClass,
                                                            metadataClass: MClass) {
        log.info("Listening for events on [{}]", streamName)

        val config = clientProvider.consumerConfig(streamName)
        val worker = workerFactory.worker(config, handler(streamName, eventHandler, dataClass, metadataClass))

        log.info("Running consumer to process stream [{}]...", streamName)

        worker.run()
    }

    private fun <D, M, DClass : Class<D>, MClass : Class<M>> handler(streamName: String, eventProcessor: (D, M) -> Unit, dataClass: DClass, metadataClass: MClass): EventHandler<D, M> {
        val type = objectMapper.typeFactory.constructParametricType(KinesisEventWrapper::class.java, dataClass, metadataClass)

        return EventHandler(streamName, type, eventProcessor)
    }

    inline fun <reified D, reified M> handler(streamName: String, noinline eventProcessor: (D, M) -> Unit): EventHandler<D, M> {

        val type = objectMapper.typeFactory.constructParametricType(KinesisEventWrapper::class.java, D::class.java, M::class.java)

        return EventHandler(streamName, type, eventProcessor)
    }

    fun <D, M> listen(handler: EventHandler<D, M>) {
        log.info("Listening for events on [{}]", handler.streamName)

        val config = clientProvider.consumerConfig(handler.streamName)
        val worker = workerFactory.worker(config, handler)

        log.info("Running consumer to process stream [{}]...", handler.streamName)

        worker.run()
    }

}

data class EventHandler<in D, in M>(val streamName: String, val eventType: JavaType, val eventHandler: (D, M) -> Unit)