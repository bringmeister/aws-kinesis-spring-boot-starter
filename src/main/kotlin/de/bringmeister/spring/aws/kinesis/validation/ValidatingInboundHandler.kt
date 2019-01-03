package de.bringmeister.spring.aws.kinesis.validation

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler.UnrecoverableException
import de.bringmeister.spring.aws.kinesis.Record
import javax.validation.ValidationException
import javax.validation.Validator

class ValidatingInboundHandler(
    private val delegate: KinesisInboundHandler,
    private val validator: Validator
) : KinesisInboundHandler by delegate {

    override fun handleRecord(record: Record<*, *>, context: KinesisInboundHandler.ExecutionContext) {
        val violations = validator.validate(record.data)
        if (violations.isNotEmpty()) {
            throw UnrecoverableException(ValidationException("$violations"))
        }
        delegate.handleRecord(record, context)
    }
}
