package de.bringmeister.spring.aws.kinesis.health

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandlerPostProcessor

class WorkerHealthInboundHandlerPostProcessor(
    private val composite: CompositeKinesisHealthIndicator
) : KinesisInboundHandlerPostProcessor {

    override fun postProcess(handler: KinesisInboundHandler<*, *>): KinesisInboundHandler<*, *> {
        val decorated = WorkerHealthInboundHandler(handler)
        composite.registerWorker(decorated)
        return decorated
    }
}
