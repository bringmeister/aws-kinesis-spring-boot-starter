package de.bringmeister.spring.aws.kinesis.retry

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandlerPostProcessor
import javax.annotation.Priority

@Priority(RetryableRecordHandlerPostProcessor.priority)
class RetryableRecordHandlerPostProcessor(val settings: RetryableRecordProcessorSettings) : KinesisInboundHandlerPostProcessor {

    companion object {
        const val priority = 2000
    }

    override fun postProcess(handler: KinesisInboundHandler<*, *>) = RetryableRecordHandler(settings, handler)
}