package de.bringmeister.spring.aws.kinesis

import org.slf4j.LoggerFactory

class AwsKinesisInboundGateway(
    private val workerFactory: WorkerFactory,
    private val workerStarter: WorkerStarter,
    private val recordDeserializerFactory: RecordDeserializerFactory,
    private val handlerPostProcessors: List<KinesisInboundHandlerPostProcessor> = emptyList()
) {

    private val log = LoggerFactory.getLogger(this.javaClass)

    fun <D, M> register(handler: KinesisInboundHandler<D, M>) {

        val decorated = handlerPostProcessors.fold(handler) { it, postProcessor ->
            postProcessor.postProcess(it)
        }
        val recordDeserializer = recordDeserializerFactory.deserializerFor(decorated)
        val worker = workerFactory.worker(decorated, recordDeserializer)
        workerStarter.start(worker)
        log.info("Started AWS Kinesis listener. [stream={}]", handler.stream)
    }
}
