package de.bringmeister.spring.aws.kinesis

import software.amazon.kinesis.coordinator.Scheduler
import org.slf4j.LoggerFactory

class AwsKinesisInboundGateway(
    private val workerFactory: WorkerFactory,
    private val workerStarter: WorkerStarter,
    private val recordDeserializerFactory: RecordDeserializerFactory,
    private val handlerPostProcessors: Iterable<KinesisInboundHandlerPostProcessor> = emptyList()
) {

    private val log = LoggerFactory.getLogger(this.javaClass)

    fun register(handler: KinesisInboundHandler<*, *>) {

        val decorated = handlerPostProcessors.fold(handler) { it, postProcessor ->
            log.debug("Applying post processor <{}> on inbound handler <{}>", postProcessor, it)
            postProcessor.postProcess(it)
        }
        val worker = worker(decorated)
        workerStarter.startWorker(handler.stream, worker)
        log.info("Kinesis listener for stream <{}> registered.", handler.stream)
    }

    private fun <D, M> worker(handler: KinesisInboundHandler<D, M>): Scheduler {
        val recordDeserializer = recordDeserializerFactory.deserializerFor(handler)
        return workerFactory.worker(handler, recordDeserializer)
    }
}
