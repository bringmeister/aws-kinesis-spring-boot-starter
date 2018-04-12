package de.bringmeister.spring.aws.kinesis

import org.slf4j.LoggerFactory

class AwsKinesisInboundGateway(private val clientProvider: AwsKinesisClientProvider,
                               private val workerFactory: WorkerFactory) {

    private val log = LoggerFactory.getLogger(this.javaClass)

    fun <PayloadType, KinesisEventType : KinesisEvent<PayloadType, *>, KinesisEventClassType : Class<KinesisEventType>>
            listen(streamName: String, eventClass: KinesisEventClassType, payloadHandler: (PayloadType) -> Unit) {

        log.info("Listening for events on [{}]", streamName)

        val config = clientProvider.consumerConfig(streamName)
        val worker = workerFactory.worker(config, eventClass, payloadHandler)

        log.info("Running consumer to process stream [{}]...", streamName)

        worker.run()
    }
}