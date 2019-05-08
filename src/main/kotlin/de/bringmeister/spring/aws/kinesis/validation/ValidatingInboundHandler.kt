package de.bringmeister.spring.aws.kinesis.validation

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.Record
import javax.validation.ValidationException
import javax.validation.Validator

class ValidatingInboundHandler<D, M>(
    private val delegate: KinesisInboundHandler<D, M>,
    private val validator: Validator
) : KinesisInboundHandler<D, M> by delegate {

    override fun handleRecord(record: Record<D, M>, context: KinesisInboundHandler.ExecutionContext) {
        val violations = validator.validate(record.data)
        if (violations.isNotEmpty()) {
            throw ValidationException("$violations")
        }
        delegate.handleRecord(record, context)
    }
}
