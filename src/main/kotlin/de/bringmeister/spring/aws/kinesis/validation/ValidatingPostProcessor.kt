package de.bringmeister.spring.aws.kinesis.validation

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandlerPostProcessor
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStreamPostProcessor
import javax.validation.Validator

class ValidatingPostProcessor(
    private val validator: Validator
) : KinesisInboundHandlerPostProcessor, KinesisOutboundStreamPostProcessor {

    override fun postProcess(handler: KinesisInboundHandler) =
        ValidatingInboundHandler(handler, validator)

    override fun postProcess(stream: KinesisOutboundStream) =
        ValidatingOutboundStream(stream, validator)
}
