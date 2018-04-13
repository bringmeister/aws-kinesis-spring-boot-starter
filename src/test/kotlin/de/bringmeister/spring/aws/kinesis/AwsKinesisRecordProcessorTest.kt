package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput
import com.amazonaws.services.kinesis.model.Record
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.doThrow
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import org.junit.Before
import org.junit.Test
import java.nio.ByteBuffer

typealias EventProcessor<D, M> = (D, M) -> Unit

class AwsKinesisRecordProcessorTest {

    val objectMapper = mock<ObjectMapper> { }
    val streamCheckpointer = mock<IRecordProcessorCheckpointer> {}
    val configuration = mock<RecordProcessorConfiguration> {
        on { maxRetries } doReturn 1
        on { backoffTimeInMilliSeconds } doReturn 1
    }

    val eventType = mock<JavaType> { }
    val eventHandler = mock<EventProcessor<FooCreatedEvent, EventMetadata>> { }
    val handler = mock<EventHandler<FooCreatedEvent, EventMetadata>> {
        on { this.eventType } doReturn eventType
        on { this.eventHandler } doReturn eventHandler
    }

    val unit = AwsKinesisRecordProcessor(objectMapper, configuration, handler)

    @Before
    fun setUp() {
        val initializationInput = mock<InitializationInput> {
            on { shardId }.thenReturn("any-shard")
        }
        unit.initialize(initializationInput)
    }

    @Test
    fun `should deserialize one record`() {
        val kinesisEvent = """{"data":"{"name":"any-value"}"}"""

        unit.processRecords(wrap(kinesisEvent))

        verify(objectMapper).readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(kinesisEvent, eventType)
    }

    @Test
    fun `should deserialize one record with metadata`() {
        val kinesisEvent = """{"metadata":"{"anything":"any-metadata"}", "data":"{"name":"any-value"}"}"""

        unit.processRecords(wrap(kinesisEvent))

        verify(objectMapper).readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(kinesisEvent, eventType)
    }

    @Test
    fun `should deserialize all records`() {
        val oneKinesisEvent = """{"data":"{"name":"any-value"}"}"""
        val anotherKinesisEvent = """{"data":"{"name":"any-other-value"}"}"""

        unit.processRecords(wrap(oneKinesisEvent, anotherKinesisEvent))

        verify(objectMapper).readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(oneKinesisEvent, eventType)
        verify(objectMapper).readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(anotherKinesisEvent, eventType)
    }

    @Test
    fun `should delegate kinesis event payload to event handler`() {
        val (eventJson, event) = eventPair()
        whenever(objectMapper.readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(eventJson, eventType)).thenReturn(event)

        unit.processRecords(wrap(eventJson))

        verify(eventHandler).invoke(event.data, event.metadata)
    }

    @Test
    fun `should delegate all kinesis event payloads to event handler`() {
        val (firstEventJson, firstEvent) = eventPair()
        val (secondEventJson, secondEvent) = Pair("""{"data":"{"name":"other-value"}"}""", KinesisEventWrapper<FooCreatedEvent, EventMetadata>(streamName = "foo-stream", data = FooCreatedEvent(Foo("other-value")), metadata = mock { }))
        whenever(objectMapper.readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(firstEventJson, eventType)).thenReturn(firstEvent)
        whenever(objectMapper.readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(secondEventJson, eventType)).thenReturn(secondEvent)

        unit.processRecords(wrap(firstEventJson, secondEventJson))

        verify(eventHandler).invoke(firstEvent.data, firstEvent.metadata)
        verify(eventHandler).invoke(secondEvent.data, secondEvent.metadata)
    }

    @Test
    fun `should retry processing on exception`() {
        val (eventJson, event) = eventPair()
        whenever(objectMapper.readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(eventJson, eventType)).thenReturn(event)
        whenever(configuration.maxRetries).thenReturn(2)
        whenever(eventHandler.invoke(event.data, event.metadata))
                .doThrow(RuntimeException::class)
                .thenReturn(Unit) // stop throwing

        unit.processRecords(wrap(eventJson))

        verify(eventHandler, times(2)).invoke(event.data, event.metadata)
    }

    @Test
    fun `should checkpoint after processing event batch`() {
        val (eventJson, event) = eventPair()
        whenever(objectMapper.readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(eventJson, eventType)).thenReturn(event)

        unit.processRecords(wrap(eventJson))

        verify(streamCheckpointer).checkpoint()
    }

    @Test
    fun `should retry checkpointing on dependency exception`() {
        val (eventJson, event) = eventPair()
        whenever(objectMapper.readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(eventJson, eventType)).thenReturn(event)
        whenever(configuration.maxRetries).thenReturn(2)
        whenever(streamCheckpointer.checkpoint())
                .doThrow(KinesisClientLibDependencyException::class).then { } // stop throwing

        unit.processRecords(wrap(eventJson))

        verify(streamCheckpointer, times(2)).checkpoint()
    }

    @Test
    fun `should retry checkpointing on throttling exception`() {
        val (eventJson, event) = eventPair()
        whenever(objectMapper.readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(eventJson, eventType)).thenReturn(event)
        whenever(configuration.maxRetries).thenReturn(2)
        whenever(streamCheckpointer.checkpoint())
                .doThrow(ThrottlingException::class).then { } // stop throwing

        unit.processRecords(wrap(eventJson))

        verify(streamCheckpointer, times(2)).checkpoint()
    }

    @Test
    fun `shouldn't retry checkpointing when application is shutting down`() {
        val (eventJson, event) = eventPair()
        whenever(objectMapper.readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(eventJson, eventType)).thenReturn(event)
        whenever(configuration.maxRetries).thenReturn(2)
        whenever(streamCheckpointer.checkpoint())
                .doThrow(ShutdownException::class).then { } // stop throwing

        unit.processRecords(wrap(eventJson))

        verify(streamCheckpointer).checkpoint()
    }

    @Test
    fun `shouldn't retry checkpointing on invalid state`() {
        val (eventJson, event) = eventPair()
        whenever(objectMapper.readValue<KinesisEventWrapper<FooCreatedEvent, EventMetadata>>(eventJson, eventType)).thenReturn(event)
        whenever(configuration.maxRetries).thenReturn(2)
        whenever(streamCheckpointer.checkpoint())
                .doThrow(InvalidStateException::class).then { } // stop throwing

        unit.processRecords(wrap(eventJson))

        verify(streamCheckpointer).checkpoint()
    }

    @Test
    fun `should checkpoint on resharding`() {
        val shutdownInput = mock<ShutdownInput> {
            on { shutdownReason } doReturn ShutdownReason.TERMINATE // re-sharding
            on { checkpointer } doReturn streamCheckpointer
        }
        unit.shutdown(shutdownInput)

        verify(streamCheckpointer).checkpoint()
    }

    private fun eventPair() = Pair("""{"data":"{"name":"any-value"}"}""", KinesisEventWrapper<FooCreatedEvent, EventMetadata>("foo-stream", data = FooCreatedEvent(Foo("any-value")), metadata = mock { }))
    private fun wrap(vararg kinesisEvents: String): ProcessRecordsInput {
        return mock {
            val eventRecords = kinesisEvents.toList().map { event -> mock<Record> { on { data } doReturn ByteBuffer.wrap(event.toByteArray()) } }
            on { records }.thenReturn(eventRecords)
            on { checkpointer }.thenReturn(streamCheckpointer)
        }
    }
}