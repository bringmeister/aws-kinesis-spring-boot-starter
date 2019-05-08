package de.bringmeister.spring.aws.kinesis.retry

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.Record
import de.bringmeister.spring.aws.kinesis.RetrySettings.Companion.NO_RETRIES
import de.bringmeister.spring.aws.kinesis.retry.RetryableRecordProcessorSettings.Companion.INFINITE_RETRIES
import org.slf4j.LoggerFactory

class RetryableRecordHandler<D, M>(private val settings: RetryableRecordProcessorSettings, private val delegate: KinesisInboundHandler<D, M>) :
    KinesisInboundHandler<D, M> by delegate {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun handleRecord(record: Record<D, M>, context: KinesisInboundHandler.ExecutionContext) {

        when {
            settings.maxRetries < -1 -> {
                throw IllegalArgumentException("Unsupported value for aws.kinesis.handler.retry.maxRetries: [${settings.maxRetries}].")
            }
            settings.maxRetries == NO_RETRIES -> try {
                log.debug("Processing record [{}] without retries.", context.sequenceNumber)
                delegate.handleRecord(record, context)
            } catch (e: Throwable) {
                log.warn("Failed to process record [{}]. Skipping record.", context.sequenceNumber, e)
            }
            settings.maxRetries == INFINITE_RETRIES -> {
                var attempt = 1
                while (true) {
                    try {
                        log.debug("Processing record [{}] with infinite retries. Attempt [{}].", context.sequenceNumber, attempt++, record.data)
                        delegate.handleRecord(record, context)
                        return // successful processed, end retry loop
                    } catch (e: Throwable) {
                        log.warn("Failed to process record [{}]. Waiting [{}] before retrying it.", context.sequenceNumber, settings.backoff, e)
                        backoff()
                    }
                }
            }
            else -> {
                val maxAttempts = 1 + settings.maxRetries
                for (attempt in 1..maxAttempts) {
                    try {
                        log.debug("Processing record [{}] with max retries of [{}]. Attempt [{}] of [{}]", context.sequenceNumber, settings.maxRetries, attempt, maxAttempts)
                        delegate.handleRecord(record, context)
                        return // successful processed, end retry loop
                    } catch (e: Throwable) {
                        log.warn("Failed to process record [{}]. Waiting [{}] before retrying it.", context.sequenceNumber, settings.backoff, e)
                        backoff()
                    }
                }
                log.error(
                    "Failed to process record [{}] after [{}] attempts. Max retries of [{}] exceeded. Skipping record.",
                    context.sequenceNumber,
                    maxAttempts,
                    settings.maxRetries
                )
            }
        }
    }

    private fun backoff() {
        Thread.sleep(settings.backoff.toMillis())
    }
}