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
import java.nio.charset.Charset
import javax.validation.Validator

class AwsKinesisRecordProcessor(
    private val recordMapper: RecordMapper,
    private val configuration: RecordProcessorConfiguration,
    private val handler: KinesisListenerProxy,
    private val validator: Validator? = null
) : IRecordProcessor {

    private val log = LoggerFactory.getLogger(javaClass.name)

    override fun initialize(initializationInput: InitializationInput?) {
        log.info("Initializing worker for stream [{}] and shard [{}]", handler.stream, initializationInput!!.shardId)
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput?) {
        processRecordsWithRetries(processRecordsInput!!.records)
        checkpoint(processRecordsInput.checkpointer)
    }

    private fun processRecordsWithRetries(records: List<Record>) {
        log.trace("Received [{}] records on stream [{}]", records.size, handler.stream)
        for (record in records) {
            var processedSuccessfully = false
            val recordData = Charset.forName("UTF-8")
                .decode(record.data)
                .toString()

            log.trace("Stream [{}]: \nData [{}]", handler.stream, recordData)

            val maxAttempts = 1 + configuration.maxRetries
            for (attempt in 1..maxAttempts) {
                try {
                    processRecord(recordData)
                    processedSuccessfully = true
                    break
                } catch (e: Exception) {
                    log.error(
                        "Exception while processing record. [sequenceNumber=${record.sequenceNumber}, partitionKey=${record.partitionKey}]",
                        e
                    )
                }

                backoff()
            }

            if (!processedSuccessfully) {
                log.warn("Processing of record failed. Skipping it. [sequenceNumber=${record.sequenceNumber}, partitionKey=${record.partitionKey}, attempts=$maxAttempts")
            }
        }
    }

    private fun processRecord(recordData: String) {
        log.debug("Processing record.")
        val message = recordMapper.deserializeFor(recordData, handler)
        val violations = validator?.validate(message) ?: setOf()
        if (violations.isNotEmpty()) {
            log.warn("skip invalid record with violations: $violations message: $message")
            return
        }
        
        handler.invoke(message.data, message.metadata)
    }

    private fun checkpoint(checkpointer: IRecordProcessorCheckpointer) {
        log.debug("Checkpointing")
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