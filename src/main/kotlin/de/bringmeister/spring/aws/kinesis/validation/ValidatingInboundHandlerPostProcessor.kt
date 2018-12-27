package de.bringmeister.spring.aws.kinesis.validation

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandlerPostProcessor
import javax.validation.Validator

class ValidatingInboundHandlerPostProcessor(
    private val validator: Validator
) : KinesisInboundHandlerPostProcessor {
    override fun postProcess(handler: KinesisInboundHandler) =
        ValidatingInboundHandler(handler, validator)
}
