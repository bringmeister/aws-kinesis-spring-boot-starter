package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import com.amazonaws.services.kinesis.model.Record as AwsRecord
import de.bringmeister.spring.aws.kinesis.Record as BmRecord

class AwsKinesisRecordProcessor<D, M>(
    private val recordDeserializer: RecordDeserializer<D, M>,
    private val configuration: RecordProcessorConfiguration,
    private val handler: KinesisInboundHandler<D, M>,
    private val publisher: ApplicationEventPublisher
) : IRecordProcessor {

    private val log = LoggerFactory.getLogger(javaClass.name)

    override fun initialize(initializationInput: InitializationInput) {
        val workerInitializedEvent = WorkerInitializedEvent(handler.stream, initializationInput.shardId)
        publisher.publishEvent(workerInitializedEvent)
        log.info("Kinesis listener initialized: [stream={}, shardId={}]", handler.stream, initializationInput.shardId)
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput) {
        processRecordsWithRetries(processRecordsInput.records)
        checkpoint(processRecordsInput.checkpointer)
    }

    private fun processRecordsWithRetries(awsRecords: List<AwsRecord>) {
        log.trace("Received [{}] records on stream [{}]", awsRecords.size, handler.stream)
        awsRecords.forEach(this::processRecordWithRetries)
    }

    private fun processRecordWithRetries(awsRecord: AwsRecord) {
        log.trace("Stream [{}], Seq. No [{}]", handler.stream, awsRecord.sequenceNumber)

        val maxAttempts = 1 + configuration.maxRetries
        var context = AwsExecutionContext()
        try {
            val record = recordDeserializer.deserialize(awsRecord)

            for (attempt in 1..maxAttempts) {
                try {
                    handler.handleRecord(record, context)
                    return
                } catch (e: KinesisInboundHandler.UnrecoverableException) {
                    log.error(
                        "Unrecoverable exception while processing record. [sequenceNumber=${awsRecord.sequenceNumber}, partitionKey=${awsRecord.partitionKey}]",
                        e.cause
                    )
                    return
                } catch (e: Exception) {
                    log.error(
                        "Exception while processing record. [sequenceNumber=${awsRecord.sequenceNumber}, partitionKey=${awsRecord.partitionKey}]",
                        e
                    )
                }

                backoff()
                context = context.withRetryAttempt(attempt)
            }
        } catch (transformationException: Exception) {
            log.error(
                "Exception while transforming record. [sequenceNumber=${awsRecord.sequenceNumber}, partitionKey=${awsRecord.partitionKey}]",
                transformationException
            )
            try {
                handler.handleDeserializationError(transformationException, awsRecord.data.asReadOnlyBuffer(), context)
            } catch (ex: Throwable) {
                log.error(
                    "Error occurred in handler during call to handleDeserializationError for stream <{}> [sequenceNumber={}, partitionKey={}]",
                    handler.stream, awsRecord.sequenceNumber, awsRecord.partitionKey, ex)
            }
        }

        log.warn("Processing of record failed. Skipping it. [sequenceNumber=${awsRecord.sequenceNumber}, partitionKey=${awsRecord.partitionKey}, attempts=$maxAttempts")
    }

    private fun checkpoint(checkpointer: IRecordProcessorCheckpointer) {
        val maxAttempts = 1 + configuration.maxRetries
        for (attempt in 1..maxAttempts) {
            try {
                checkpointer.checkpoint()
                break
            } catch (e: ThrottlingException) {
                if (attempt == maxAttempts) {
                    log.error("Couldn't store checkpoint after max attempts of [{}].", maxAttempts, e)
                    break
                }
                log.warn("Transient issue during checkpointing - attempt $attempt of $maxAttempts", e)
            } catch (e: KinesisClientLibDependencyException) {
                if (attempt == maxAttempts) {
                    log.error("Couldn't store checkpoint after max retries.", e)
                    break
                }
                log.warn("Transient issue during checkpointing - attempt $attempt of $maxAttempts", e)
            } catch (se: ShutdownException) {
                log.info("Application is shutting down. Skipping checkpoint.", se)
                break
            } catch (e: InvalidStateException) {
                log.error("Cannot save checkpoint. Please check corresponding DynamoDB table.", e)
                break
            }

            backoff()
        }
    }

    private fun backoff() {
        try {
            Thread.sleep(configuration.backoffTimeInMilliSeconds)
        } catch (e: InterruptedException) {
            log.debug("Interrupted sleep", e)
        }
    }

    override fun shutdown(shutdownInput: ShutdownInput) {
        log.info("Shutting down record processor")
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (shutdownInput.shutdownReason == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.checkpointer)
        }
    }

    private data class AwsExecutionContext(
        private val retryAttempt: Int = 0
    ) : KinesisInboundHandler.ExecutionContext {

        override val isRetry get() = retryAttempt > 0

        fun withRetryAttempt(retryAttempt: Int) = AwsExecutionContext(retryAttempt)
    }
}
