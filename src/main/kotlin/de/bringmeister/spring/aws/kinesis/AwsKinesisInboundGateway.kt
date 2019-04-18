package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
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
        workerStarter.start(handler.stream, worker)
        log.info("Started AWS Kinesis listener. [stream={}]", handler.stream)
    }

    private fun <D, M> worker(handler: KinesisInboundHandler<D, M>): Worker {
        val recordDeserializer = recordDeserializerFactory.deserializerFor(handler)
        return workerFactory.worker(handler, recordDeserializer)
    }
}
