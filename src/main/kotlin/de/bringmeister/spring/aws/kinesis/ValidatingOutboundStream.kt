package de.bringmeister.spring.aws.kinesis

import javax.validation.ValidationException
import javax.validation.Validator

class ValidatingOutboundStream(
    private val delegate: KinesisOutboundStream,
    private val validator: Validator
) : KinesisOutboundStream by delegate {

    override fun send(vararg records: Record<*, *>) {
        val violations = validator.validate(records)
        if (violations.isNotEmpty()) {
            throw ValidationException("invalid records: $violations")
        }
        return delegate.send(records)
    }
}
