package de.bringmeister.spring.aws.kinesis.validation

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandlerPostProcessor
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStreamPostProcessor
import javax.annotation.Priority
import javax.validation.Validator

@Priority(ValidatingPostProcessor.priority)
class ValidatingPostProcessor(
    private val validator: Validator
) : KinesisInboundHandlerPostProcessor, KinesisOutboundStreamPostProcessor {

    companion object {
        const val priority = -1000
    }

    override fun postProcess(handler: KinesisInboundHandler<*, *>) =
        ValidatingInboundHandler(handler, validator)

    override fun postProcess(stream: KinesisOutboundStream) =
        ValidatingOutboundStream(stream, validator)
}
