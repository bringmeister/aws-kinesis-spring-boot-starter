package de.bringmeister.spring.aws.kinesis

import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import software.amazon.kinesis.exceptions.InvalidStateException
import software.amazon.kinesis.exceptions.KinesisClientLibNonRetryableException
import software.amazon.kinesis.exceptions.KinesisClientLibRetryableException
import software.amazon.kinesis.exceptions.ShutdownException
import software.amazon.kinesis.lifecycle.events.*
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.processor.ShardRecordProcessor
import software.amazon.kinesis.retrieval.KinesisClientRecord

class AwsKinesisRecordProcessor<D, M>(
    private val recordDeserializer: RecordDeserializer<D, M>,
    private val configuration: RecordProcessorConfiguration,
    private val handler: KinesisInboundHandler<D, M>,
    private val publisher: ApplicationEventPublisher
) : ShardRecordProcessor {

    private val log = LoggerFactory.getLogger(javaClass.name)

    private lateinit var shardId: String

    override fun initialize(initializationInput: InitializationInput) {
        shardId = initializationInput.shardId()
        val workerInitializedEvent = WorkerInitializedEvent(handler.stream, shardId)
        publisher.publishEvent(workerInitializedEvent)
        log.info("Kinesis listener initialized for shard <{}> on <{}>", shardId, handler.stream)
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput) {
        val records = processRecordsInput.records()
        val checkpointer = processRecordsInput.checkpointer()
        log.trace("Received [{}] records on shard <{}> of <{}>.", records.size, shardId, handler.stream)
        if (handler.isBatch()) {
            processRecordsBatch(processRecordsInput, checkpointer)
        } else {
            processRecordsSingle(processRecordsInput, checkpointer)
        }
    }

    private fun processRecordsBatch(
        processRecordsInput: ProcessRecordsInput,
        checkpointer: RecordProcessorCheckpointer
    ) {
        handler.handleRecords(
            processRecordsInput.records().mapNotNull { toRecord(it)?.first },
            toLastExecutionContext(processRecordsInput)
        )
        checkpoint(checkpointer)
    }

    private fun toLastExecutionContext(processRecordsInput: ProcessRecordsInput) =
        AwsExecutionContext(
            shardId,
            processRecordsInput.records().maxBy { it.sequenceNumber() }!!.sequenceNumber()
        )

    private fun processRecordsSingle(
        processRecordsInput: ProcessRecordsInput,
        checkpointer: RecordProcessorCheckpointer
    ) {
        processRecordsInput.records().forEach { clientRecord ->
            toRecord(clientRecord)?.let { handler.handleRecord(it.first, it.second) }
            if (configuration.checkpointing.strategy == CheckpointingStrategy.RECORD) {
                checkpoint(checkpointer, clientRecord)
            }
        }
        if (configuration.checkpointing.strategy == CheckpointingStrategy.BATCH) {
            checkpoint(checkpointer)
        }
    }

    private fun toRecord(awsRecord: KinesisClientRecord): Pair<Record<D, M>, AwsExecutionContext>? {
        val sequenceNumber = awsRecord.sequenceNumber()
        val partitionKey = awsRecord.partitionKey()
        log.trace(
            "Processing record at sequence number <{}> on shard <{}> of <{}>...",
            sequenceNumber,
            shardId,
            handler.stream
        )
        val context = AwsExecutionContext(shardId = shardId, sequenceNumber = sequenceNumber)

        val record = try {
            recordDeserializer.deserialize(awsRecord)
        } catch (deserializationException: Exception) {
            log.error(
                "Exception while deserializing record on stream <{}>. [sequenceNumber={}, partitionKey={}]",
                handler.stream, sequenceNumber, partitionKey,
                deserializationException
            )
            try {
                handler.handleDeserializationError(
                    deserializationException,
                    awsRecord.data().asReadOnlyBuffer(),
                    context
                )
            } catch (e: Throwable) {
                log.error(
                    "Error occurred in handler during call to handleDeserializationError for stream <{}> [sequenceNumber={}, partitionKey={}]",
                    handler.stream, sequenceNumber, partitionKey, e
                )
            }
            null
        }
        return record?.let { Pair(it, context) }
    }

    private fun checkpoint(checkpointer: RecordProcessorCheckpointer, record: KinesisClientRecord? = null) {
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
                    log.error(
                        "Checkpointing failed. Couldn't store checkpoint after max attempts of [{}].",
                        maxAttempts,
                        e
                    )
                    break
                }
                log.warn(
                    "Checkpointing failed. Transient issue during checkpointing. Attempt [{}] of [{}]",
                    attempt,
                    maxAttempts,
                    e
                )
                backoff()
            } catch (e: KinesisClientLibNonRetryableException) {
                when (e) {
                    is ShutdownException -> log.debug("Checkpointing failed. Application is shutting down.", e)
                    is InvalidStateException -> log.error(
                        "Checkpointing failed. Please check corresponding DynamoDB table.",
                        e
                    )
                    else -> log.error("Checkpointing failed. Unknown KinesisClientLibNonRetryableException.", e)
                }
                break // break retry loop
            }
        }
    }

    private fun checkpointSingleRecord(checkpointer: RecordProcessorCheckpointer, record: KinesisClientRecord) {
        val sequenceNumber = record.sequenceNumber()
        log.debug(
            "Checkpointing record at sequence number <{}> on shard <{}> of <{}>...",
            sequenceNumber,
            shardId,
            handler.stream
        )
        checkpointer.checkpoint(sequenceNumber)
    }

    private fun checkpointBatch(checkpointer: RecordProcessorCheckpointer) {
        log.debug("Checkpointing batch on shard <{}> of <{}>...", shardId, handler.stream)
        checkpointer.checkpoint()
    }

    private fun backoff() {
        try {
            Thread.sleep(configuration.checkpointing.backoff.toMillis())
        } catch (e: InterruptedException) {
            log.debug("Interrupted sleep", e)
        }
    }

    override fun leaseLost(leaseLostInput: LeaseLostInput) {
        log.info("Lease lost for shard <{}> on stream <{}>.", shardId, handler.stream)
    }

    override fun shardEnded(shardEndedInput: ShardEndedInput) {
        log.info("Shard ended for shard <{}> on stream <{}>. Checkpointing...", shardId, handler.stream)
        checkpoint(shardEndedInput.checkpointer(), null)
        log.info("Checkpointed shard <{}> on stream <{}>. Shard ended.", shardId, handler.stream)
    }

    override fun shutdownRequested(shutdownRequestedInput: ShutdownRequestedInput) {
        log.info("Shutting down record processor for shard <{}> on stream <{}>...", shardId, handler.stream)
        checkpoint(shutdownRequestedInput.checkpointer(), null)
        log.info("Record processor for shard <{}> on stream <{}> shut down successfully.", shardId, handler.stream)
    }

    private data class AwsExecutionContext(
        override val shardId: String,
        override val sequenceNumber: String
    ) : KinesisInboundHandler.ExecutionContext
}
