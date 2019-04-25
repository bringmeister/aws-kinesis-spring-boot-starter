package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibNonRetryableException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibRetryableException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import com.amazonaws.services.kinesis.model.Record as AwsRecord

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
        log.trace("Received [{}] records on stream [{}]", processRecordsInput.records.size, handler.stream)
        for (record in processRecordsInput.records) {

            processRecord(record)

            if (configuration.checkpointing.strategy == CheckpointingStrategy.RECORD) {
                checkpoint(processRecordsInput.checkpointer, record)
            }
        }

        if (configuration.checkpointing.strategy == CheckpointingStrategy.BATCH) {
            checkpoint(processRecordsInput.checkpointer)
        }
    }

    private fun processRecord(awsRecord: AwsRecord) {
        log.trace("Stream [{}], Seq. No [{}]", handler.stream, awsRecord.sequenceNumber)
        var context = AwsExecutionContext(awsRecord.sequenceNumber)

        val record: Record<D, M> = try {
            recordDeserializer.deserialize(awsRecord)
        } catch (deserializationException: Exception) {
            log.error("Exception while deserializing record. [sequenceNumber=${awsRecord.sequenceNumber}, partitionKey=${awsRecord.partitionKey}]", deserializationException)
            try {
                handler.handleDeserializationError(deserializationException, awsRecord.data.asReadOnlyBuffer(), context)
            } catch (e: Throwable) {
                log.error(
                    "Error occurred in handler during call to handleDeserializationError for stream <{}> [sequenceNumber={}, partitionKey={}]",
                    handler.stream, awsRecord.sequenceNumber, awsRecord.partitionKey, e
                )
            }
            return
        }

        handler.handleRecord(record, context)
    }

    private fun checkpoint(checkpointer: IRecordProcessorCheckpointer, record: AwsRecord? = null) {
        val maxAttempts = 1 + configuration.checkpointing.maxRetries
        for (attempt in 1..maxAttempts) {
            try {
                if (record == null) {
                    checkpointBatch(checkpointer)
                } else {
                    checkpointSingleRecord(checkpointer, record)
                }
                break
            } catch (e: KinesisClientLibRetryableException) {
                if (attempt == maxAttempts) {
                    log.error("Checkpointing failed. Couldn't store checkpoint after max attempts of [{}].", maxAttempts, e)
                    break
                }
                log.warn("Checkpointing failed. Transient issue during checkpointing. Attempt [{}] of [{}]", attempt, maxAttempts, e)
                backoff()
            } catch (e: KinesisClientLibNonRetryableException) {
                when (e) {
                    is ShutdownException -> log.debug("Checkpointing failed. Application is shutting down.", e)
                    is InvalidStateException -> log.error("Checkpointing failed. Please check corresponding DynamoDB table.", e)
                    else -> {
                        log.error("Checkpointing failed. Unknown KinesisClientLibNonRetryableException.", e)
                    }
                }
                break // break retry loop
            }
        }
    }

    private fun checkpointSingleRecord(checkpointer: IRecordProcessorCheckpointer, record: AwsRecord) {
        log.debug("Checkpointing record at [{}]", record.sequenceNumber)
        checkpointer.checkpoint(record)
    }

    private fun checkpointBatch(checkpointer: IRecordProcessorCheckpointer) {
        log.debug("Checkpointing batch")
        checkpointer.checkpoint()
    }

    private fun backoff() {
        try {
            Thread.sleep(configuration.checkpointing.backoff.toMillis())
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

    private data class AwsExecutionContext(override val sequenceNumber: String) : KinesisInboundHandler.ExecutionContext
}
