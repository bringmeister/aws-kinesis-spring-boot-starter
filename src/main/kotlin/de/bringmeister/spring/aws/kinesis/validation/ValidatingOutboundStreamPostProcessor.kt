package de.bringmeister.spring.aws.kinesis.validation

import de.bringmeister.spring.aws.kinesis.KinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStreamPostProcessor
import javax.validation.Validator

class ValidatingOutboundStreamPostProcessor(
    private val validator: Validator
) : KinesisOutboundStreamPostProcessor {
    override fun postProcess(stream: KinesisOutboundStream) =
        ValidatingOutboundStream(stream, validator)
}
