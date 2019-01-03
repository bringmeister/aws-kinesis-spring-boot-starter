package de.bringmeister.spring.aws.kinesis

import org.slf4j.LoggerFactory

class AwsKinesisInboundGateway(
    private val workerFactory: WorkerFactory,
    private val workerStarter: WorkerStarter
) {

    private val log = LoggerFactory.getLogger(this.javaClass)

    fun register(handler: KinesisInboundHandler, recordDeserializer: RecordDeserializer) {
        val worker = workerFactory.worker(handler, recordDeserializer)
        workerStarter.start(worker)
        log.info("Started AWS Kinesis listener. [stream={}]", handler.stream)
    }
}
