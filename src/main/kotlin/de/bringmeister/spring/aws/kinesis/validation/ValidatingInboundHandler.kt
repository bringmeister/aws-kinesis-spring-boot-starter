package de.bringmeister.spring.aws.kinesis.validation

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler.UnrecoverableException
import javax.validation.ValidationException
import javax.validation.Validator

class ValidatingInboundHandler(
    private val delegate: KinesisInboundHandler,
    private val validator: Validator
) : KinesisInboundHandler by delegate {

    override fun handleMessage(data: Any?, metadata: Any?) {
        val violations = validator.validate(data)
        if (violations.isNotEmpty()) {
            throw UnrecoverableException(ValidationException("$violations"))
        }
        delegate.handleMessage(data, metadata)
    }
}
