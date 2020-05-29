package de.bringmeister.spring.aws.kinesis

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.argumentCaptor
import com.nhaarman.mockito_kotlin.doAnswer
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.doThrow
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.verifyNoMoreInteractions
import com.nhaarman.mockito_kotlin.verifyZeroInteractions
import com.nhaarman.mockito_kotlin.whenever
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.Test
import org.springframework.context.ApplicationEventPublisher
import software.amazon.kinesis.exceptions.KinesisClientLibNonRetryableException
import software.amazon.kinesis.exceptions.KinesisClientLibRetryableException
import software.amazon.kinesis.exceptions.ThrottlingException
import software.amazon.kinesis.lifecycle.events.LeaseLostInput
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput
import software.amazon.kinesis.lifecycle.events.ShardEndedInput
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput
import software.amazon.kinesis.processor.RecordProcessorCheckpointer
import software.amazon.kinesis.retrieval.KinesisClientRecord
import java.nio.ByteBuffer
import java.time.Duration
import java.util.UUID

class AwsKinesisRecordProcessorTest {

    private data class ExampleEvent(val data: ExampleData, val metadata: ExampleMetadata)
    private data class ExampleData(val value: String)
    private data class ExampleMetadata(val hash: String)

    private class ExampleException : RuntimeException()

    private val checkpointMaxRetries = 2
    private val checkpointingBackoff = Duration.ofMillis(1)
    private val exampleConfig = RecordProcessorConfiguration(
        checkpointing = CheckpointingConfiguration(maxRetries = checkpointMaxRetries, backoff = checkpointingBackoff)
    )

    private val mockHandler = mock<KinesisInboundHandler<ExampleData, ExampleMetadata>> {
        on { dataType() } doReturn ExampleData::class.java
        on { metaType() } doReturn ExampleMetadata::class.java
        on { stream } doReturn "any-stream"
    }

    private val mockCheckpointer = mock<RecordProcessorCheckpointer> {}
    private val mockEventPublisher = mock<ApplicationEventPublisher> {}
    private val objectMapper = ObjectMapper().registerModule(KotlinModule())
    private val recordDeserializer = ObjectMapperRecordDeserializerFactory(objectMapper).deserializerFor(mockHandler)

    private fun recordProcessor(configuration: RecordProcessorConfiguration = exampleConfig): AwsKinesisRecordProcessor<ExampleData, ExampleMetadata> {
        return AwsKinesisRecordProcessor(recordDeserializer, configuration, mockHandler, mockEventPublisher)
    }

    private val events = listOf(
        ExampleEvent(ExampleData("first"), ExampleMetadata("8b04d5e3775d298e78455efc5ca404d5")),
        ExampleEvent(ExampleData("second"), ExampleMetadata("a9f0e61a137d86aa9db53465e0801612"))
    )
    private val eventsAsJson = events.map(this::json)

    /* ---- Happy Path ---- */
    @Test
    fun `should invoke event handler for every record of a batch`() {
        val records = recordBatch(eventsAsJson)

        recordProcessor().processRecords(records)

        verifyAllEventsAreProcessed()
        verify(mockCheckpointer).checkpoint()
    }

    /* ---- Initialization ---- */
    @Test
    fun `should publish event on initialization`() {
        recordProcessor().initialize(mock { on { shardId() } doReturn "any" })

        val captor = argumentCaptor<WorkerInitializedEvent>()
        verify(mockEventPublisher).publishEvent(captor.capture())
        assertThat(captor.firstValue).isInstanceOf(WorkerInitializedEvent::class.java)
    }

    /* ---- Checkpointing - complete batch ---- */
    @Test
    fun `should checkpoint batch after all records of that batch are processed`() {
        val records = recordBatch(eventsAsJson)
        val config = RecordProcessorConfiguration(checkpointing = CheckpointingConfiguration(checkpointMaxRetries, checkpointingBackoff, CheckpointingStrategy.BATCH))

        recordProcessor(config).processRecords(records)

        verify(mockCheckpointer).checkpoint()
        verifyNoMoreInteractions(mockCheckpointer)
    }

    @Test
    fun `should not checkpoint batch if a record can't be processed`() {
        val records = recordBatch(eventsAsJson)

        whenever(mockHandler.handleRecord(any(), any()))
            .then { } // process 1st record successful
            .doThrow(ExampleException::class) // fail on 2nd record
            .then { } // process 3rd record successful

        Assertions.catchThrowable {
            val config = RecordProcessorConfiguration(
                checkpointing = CheckpointingConfiguration(checkpointMaxRetries, checkpointingBackoff, CheckpointingStrategy.BATCH)
            )
            recordProcessor(config).processRecords(records)
        }
        verifyZeroInteractions(mockCheckpointer)
    }

    /* ---- Checkpointing - single records ---- */
    @Test
    fun `should checkpoint after every record when checkpointing strategy is set to 'RECORD'`() {
        val records = recordBatch(eventsAsJson)
        val config = RecordProcessorConfiguration(checkpointing = CheckpointingConfiguration(checkpointMaxRetries, checkpointingBackoff, CheckpointingStrategy.RECORD))

        recordProcessor(config).processRecords(records)

        for (record in records.records()) {
            verify(mockCheckpointer).checkpoint(record.sequenceNumber())
        }
        verifyNoMoreInteractions(mockCheckpointer)
    }

    @Test
    fun `should checkpoint only successful processed records when checkpointing strategy is set to 'RECORD'`() {
        val records = recordBatch(eventsAsJson)

        whenever(mockHandler.handleRecord(any(), any()))
            .then { } // process 1st record successful
            .doThrow(ExampleException::class) // fail on 2nd record
            .then { } // process 3rd record successful

        Assertions.catchThrowable {
            val config = RecordProcessorConfiguration(
                checkpointing = CheckpointingConfiguration(checkpointMaxRetries, checkpointingBackoff, CheckpointingStrategy.RECORD)
            )
            recordProcessor(config).processRecords(records)
        }
        verify(mockCheckpointer).checkpoint(records.records()[0].sequenceNumber()) // checkpoint only successful processed record
        verifyNoMoreInteractions(mockCheckpointer)
    }

    @Test
    fun `should use 'BATCH' checkpointing strategy as default`() {
        val config = RecordProcessorConfiguration(CheckpointingConfiguration(checkpointMaxRetries, checkpointingBackoff))

        assertThat(config.checkpointing.strategy).isEqualTo(CheckpointingStrategy.BATCH)
    }

    /* ---- Deserialization failures ---- */
    @Test
    fun `should skip invalid records when deserialization fails and process remaining records`() {
        val records = recordBatch(eventsAsJson.toMutableList().apply { add(1, "{foobar}") }) // insert one invalid record between two valid ones

        recordProcessor().processRecords(records)

        verifyAllEventsAreProcessed()
    }

    @Test
    fun `should invoke deserializationErrorHandler when deserialization fails`() {
        val records = recordBatch(listOf("{foobar}"))

        recordProcessor().processRecords(records)

        verify(mockHandler).handleDeserializationError(any(), any(), any())
    }

    private class AnyKinesisRetryableException : KinesisClientLibRetryableException("any")

    @Test
    fun `should retry batch checkpointing on KinesisClientLibRetryableException`() {
        val records = recordBatch(eventsAsJson)

        whenever(mockCheckpointer.checkpoint())
            .doThrow(AnyKinesisRetryableException::class)
            .then { } // stop throwing

        val config = RecordProcessorConfiguration(checkpointing = CheckpointingConfiguration(checkpointMaxRetries, checkpointingBackoff, CheckpointingStrategy.BATCH))
        recordProcessor(config).processRecords(records)

        verify(mockCheckpointer, times(2)).checkpoint()
    }

    @Test
    fun `should retry record checkpointing on KinesisClientLibRetryableException`() {
        val records = recordBatch(eventsAsJson)

        whenever(mockCheckpointer.checkpoint(any<String>()))
            .doThrow(AnyKinesisRetryableException::class)
            .then { } // stop throwing

        val config = RecordProcessorConfiguration(checkpointing = CheckpointingConfiguration(checkpointMaxRetries, checkpointingBackoff, CheckpointingStrategy.RECORD))
        recordProcessor(config).processRecords(records)

        verify(mockCheckpointer, times(2)).checkpoint(records.records()[0].sequenceNumber()) // retry
        verify(mockCheckpointer).checkpoint(records.records()[1].sequenceNumber())
    }

    private class AnyKinesisNonRetryableException : KinesisClientLibNonRetryableException("any")

    @Test
    fun `should not retry batch checkpointing on KinesisClientLibNonRetryableException`() {
        val records = recordBatch(eventsAsJson)

        whenever(mockCheckpointer.checkpoint())
            .doAnswer { throw AnyKinesisNonRetryableException() }
            .then { } // stop throwing

        val config = RecordProcessorConfiguration(checkpointing = CheckpointingConfiguration(checkpointMaxRetries, checkpointingBackoff, CheckpointingStrategy.BATCH))
        recordProcessor(config).processRecords(records)

        verify(mockCheckpointer).checkpoint() // verify checkpoint is called only once
    }

    @Test
    fun `should not retry record checkpointing on KinesisClientLibNonRetryableException`() {
        val records = recordBatch(eventsAsJson)

        whenever(mockCheckpointer.checkpoint(any<String>()))
            .doAnswer { throw AnyKinesisNonRetryableException() }
            .then { } // stop throwing

        val config = RecordProcessorConfiguration(checkpointing = CheckpointingConfiguration(checkpointMaxRetries, checkpointingBackoff, CheckpointingStrategy.RECORD))
        recordProcessor(config).processRecords(records)

        verify(mockCheckpointer).checkpoint(records.records()[0].sequenceNumber())
        verify(mockCheckpointer).checkpoint(records.records()[1].sequenceNumber())
    }

    @Test
    fun `should not bubble exception when checkpointing on ThrottlingException runs out of retries`() {
        val records = recordBatch(eventsAsJson)
        whenever(mockCheckpointer.checkpoint()).doThrow(ThrottlingException::class)

        assertThatCode { recordProcessor().processRecords(records) }
            .doesNotThrowAnyException()
    }

    @Test
    fun `should checkpoint on shutdown request`() {
        val request = ShutdownRequestedInput.builder().checkpointer(mockCheckpointer).build()
        recordProcessor().shutdownRequested(request)
        verify(mockCheckpointer).checkpoint()
    }

    @Test
    fun `should checkpoint on shard ended`() {
        val request = ShardEndedInput.builder().checkpointer(mockCheckpointer).build()
        recordProcessor().shardEnded(request)
        verify(mockCheckpointer).checkpoint()
    }

    @Test
    fun `should not error on lease lost`() {
        val request = LeaseLostInput.builder().build()
        recordProcessor().leaseLost(request)
    }

    private fun verifyAllEventsAreProcessed() {
        for (i in events.indices) {
            val recordCaptor = argumentCaptor<Record<ExampleData, ExampleMetadata>>()
            val contextCaptor = argumentCaptor<KinesisInboundHandler.ExecutionContext>()
            verify(mockHandler, times(events.size)).handleRecord(recordCaptor.capture(), contextCaptor.capture())

            assertThat(recordCaptor.allValues[i].data).isEqualTo(events[i].data)
            assertThat(recordCaptor.allValues[i].metadata).isEqualTo(events[i].metadata)
        }
    }

    private fun json(event: ExampleEvent) = objectMapper.writeValueAsString(event)

    private fun recordBatch(recordData: List<String>): ProcessRecordsInput {
        val records = recordData.map { record ->
            KinesisClientRecord.builder()
                .sequenceNumber(UUID.randomUUID().toString())
                .data(ByteBuffer.wrap(record.toByteArray()))
                .build()
        }
        return ProcessRecordsInput.builder()
            .records(records)
            .checkpointer(mockCheckpointer)
            .build()
    }
}
