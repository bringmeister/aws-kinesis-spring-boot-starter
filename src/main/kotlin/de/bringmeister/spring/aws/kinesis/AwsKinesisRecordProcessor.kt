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
import com.amazonaws.services.kinesis.model.Record
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import java.nio.charset.Charset

class AwsKinesisRecordProcessor(
    private val recordMapper: RecordMapper,
    private val configuration: RecordProcessorConfiguration,
    private val handler: KinesisInboundHandler,
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

    private fun processRecordsWithRetries(awsRecords: List<Record>) {
        log.trace("Received [{}] records on stream [{}]", awsRecords.size, handler.stream)
        awsRecords.forEach(this::processRecordWithRetries)
    }

    private fun processRecordWithRetries(awsRecord: Record) {
        val recordJson = Charset.forName("UTF-8")
            .decode(awsRecord.data)
            .toString()

        val maxAttempts = 1 + configuration.maxRetries
        try {
            log.trace("Stream [{}], Seq. No [{}]: {}", handler.stream, awsRecord.sequenceNumber, recordJson)

            val record = getRecordFromJson(recordJson)

            for (attempt in 1..maxAttempts) {
                try {
                    handler.handleMessage(record.data, record.metadata)
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
            }
        } catch (transformationException: Exception) {
            log.error(
                "Exception while transforming record. [sequenceNumber=${awsRecord.sequenceNumber}, partitionKey=${awsRecord.partitionKey}]",
                transformationException
            )
        }

        log.warn("Processing of record failed. Skipping it. [sequenceNumber=${awsRecord.sequenceNumber}, partitionKey=${awsRecord.partitionKey}, attempts=$maxAttempts")
    }

    private fun getRecordFromJson(recordData: String): de.bringmeister.spring.aws.kinesis.Record<*, *> {
        return recordMapper.deserializeFor(recordData, handler)
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
        if (shutdownInput.shutdownReason == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.checkpointer)
        }
    }
}
