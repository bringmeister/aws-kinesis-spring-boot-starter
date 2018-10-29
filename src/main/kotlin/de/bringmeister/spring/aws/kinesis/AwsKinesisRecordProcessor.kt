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
import java.nio.charset.Charset
import javax.validation.ValidationException
import javax.validation.Validator
import com.amazonaws.services.kinesis.model.Record as AWSRecord

class AwsKinesisRecordProcessor(
    private val recordMapper: RecordMapper,
    private val configuration: RecordProcessorConfiguration,
    private val handler: KinesisListenerProxy,
    private val publisher: ApplicationEventPublisher,
    private val validator: Validator? = null
) : IRecordProcessor {

    private val log = LoggerFactory.getLogger(javaClass.name)

    override fun initialize(initializationInput: InitializationInput?) {
        val workerInitializedEvent = WorkerInitializedEvent(handler.stream, initializationInput!!.shardId)
        publisher.publishEvent(workerInitializedEvent)
        log.info("Kinesis listener initialized: [stream={}, shardId={}]", handler.stream, initializationInput.shardId)
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput?) {
        processRecordsWithRetries(processRecordsInput!!.records)
        checkpoint(processRecordsInput.checkpointer)
    }

    private fun processRecordsWithRetries(awsRecords: List<com.amazonaws.services.kinesis.model.Record>) {
        log.trace("Received [{}] records on stream [{}]", awsRecords.size, handler.stream)

        if (handler.mode == ListenerMode.BATCH) {
            handleBatch(awsRecords)
        } else {
            awsRecords.forEach(this::processRecordWithRetries)
        }
    }

    private fun handleBatch(awsRecords: List<AWSRecord>) {
        val records = toRecords(awsRecords)

        val maxAttempts = 1 + configuration.maxRetries
        for (attempt in 1..maxAttempts) {
            try {
                handler.invoke(records)
            } catch (e: Exception) {
                log.error("Exception while processing records.", e)
                backoff()
            }
        }
    }

    private fun toRecords(awsRecords: List<AWSRecord>): List<Record<*, *>> {
        return awsRecords.asSequence()
            .mapNotNull { awsRecord ->
                val record = toRecord(awsRecord)
                val violations = validator?.validate(record) ?: setOf()
                if (violations.isNotEmpty()) {
                    log.warn("filtered {} with id {} with violations: {}",
                        record!!::class.java.simpleName, awsRecord.sequenceNumber,
                        violations.map { "${it.propertyPath}: ${it.invalidValue} ${it.message}" })
                    return@mapNotNull null
                }
                return@mapNotNull record
            }
            .toList()
    }

    private fun toRecord(awsRecord: AWSRecord): Record<*, *>? {
        try {
            val recordData = Charset.forName("UTF-8")
                .decode(awsRecord.data)
                .toString()
            return recordMapper.deserializeFor(recordData, handler)
        } catch (transformationException: Exception) {
            log.error(
                "Exception while transforming record. [sequenceNumber={}, partitionKey={}]",
                awsRecord.sequenceNumber, awsRecord.partitionKey, transformationException
            )
        }
        return null
    }

    private fun processRecordWithRetries(awsRecord: AWSRecord) {
        val recordJson = Charset.forName("UTF-8")
            .decode(awsRecord.data)
            .toString()

        val maxAttempts = 1 + configuration.maxRetries
        try {
            log.trace("Stream [{}]: {}", handler.stream, recordJson)

            val record = getRecordFromJson(recordJson)

            for (attempt in 1..maxAttempts) {
                try {
                    when (handler.mode) {
                        ListenerMode.RECORD -> handler.invoke(record)
                        ListenerMode.DATA_METADATA -> handler.invoke(record.data, record.metadata)
                        else -> throw IllegalStateException("unexpected listener configuration")
                    }
                    return
                } catch (e: Exception) {
                    log.error(
                        "Exception while processing record. [sequenceNumber={}, partitionKey={}]",
                        awsRecord.sequenceNumber, awsRecord.partitionKey, e
                    )
                }

                backoff()
            }
        } catch (transformationException: Exception) {
            log.error(
                "Exception while transforming record. [sequenceNumber={}, partitionKey={}]",
                awsRecord.sequenceNumber, awsRecord.partitionKey, transformationException
            )
        }

        log.warn("Processing of record failed. Skipping it. [sequenceNumber={}, partitionKey={}], attempts={}]",
            awsRecord.sequenceNumber, awsRecord.partitionKey, maxAttempts)
    }

    private fun getRecordFromJson(recordData: String): de.bringmeister.spring.aws.kinesis.Record<*, *> {
        val record = recordMapper.deserializeFor(recordData, handler)
        val violations = validator?.validate(record) ?: setOf()
        if (violations.isNotEmpty()) {
            throw ValidationException("$violations")
        }

        return record
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

    override fun shutdown(shutdownInput: ShutdownInput?) {
        log.info("Shutting down record processor")
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (shutdownInput?.shutdownReason == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.checkpointer)
        }
    }
}
