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
import com.fasterxml.jackson.databind.ObjectMapper
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.doThrow
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import org.junit.Before
import org.junit.Test
import java.nio.ByteBuffer

typealias EventProcessor<T> = (T) -> Unit

class AwsKinesisRecordProcessorTest {

    val objectMapper = mock<ObjectMapper> { }
    val streamCheckpointer = mock<IRecordProcessorCheckpointer> {}
    val configuration = mock<RecordProcessorConfiguration> {
        on { maxRetries } doReturn 1
        on { backoffTimeInMilliSeconds } doReturn 1
    }
    val eventProcessor = mock<EventProcessor<FooCreatedEvent>> { }

    val unit = AwsKinesisRecordProcessor(objectMapper, configuration, FooCreatedKinesisEvent::class.java, eventProcessor)

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

        verify(objectMapper).readValue(kinesisEvent, FooCreatedKinesisEvent::class.java)
    }

    @Test
    fun `should deserialize one record with metadata`() {
        val kinesisEvent = """{"metadata":"{"anything":"any-metadata"}", "data":"{"name":"any-value"}"}"""

        unit.processRecords(wrap(kinesisEvent))

        verify(objectMapper).readValue(kinesisEvent, FooCreatedKinesisEvent::class.java)
    }

    @Test
    fun `should deserialize all records`() {
        val oneKinesisEvent = """{"data":"{"name":"any-value"}"}"""
        val anotherKinesisEvent = """{"data":"{"name":"any-other-value"}"}"""

        unit.processRecords(wrap(oneKinesisEvent, anotherKinesisEvent))

        verify(objectMapper).readValue(oneKinesisEvent, FooCreatedKinesisEvent::class.java)
        verify(objectMapper).readValue(anotherKinesisEvent, FooCreatedKinesisEvent::class.java)
    }

    @Test
    fun `should delegate kinesis event payload to event processor`() {
        val (kinesisEvent, kinesisEventInstance) = eventPair()
        whenever(objectMapper.readValue(kinesisEvent, FooCreatedKinesisEvent::class.java)).thenReturn(kinesisEventInstance)

        unit.processRecords(wrap(kinesisEvent))

        verify(eventProcessor).invoke(kinesisEventInstance.data)
    }

    @Test
    fun `should delegate all kinesis event payloads to event processor`() {
        val (oneKinesisEvent, oneKinesisEventInstance) = eventPair()
        val (anotherKinesisEvent, anotherKinesisEventInstance) = Pair("""{"data":"{"name":"other-value"}"}""", FooCreatedKinesisEvent(data = FooCreatedEvent(Foo("other-value")), metadata = mock { }))
        whenever(objectMapper.readValue(oneKinesisEvent, FooCreatedKinesisEvent::class.java)).thenReturn(oneKinesisEventInstance)
        whenever(objectMapper.readValue(anotherKinesisEvent, FooCreatedKinesisEvent::class.java)).thenReturn(anotherKinesisEventInstance)

        unit.processRecords(wrap(oneKinesisEvent, anotherKinesisEvent))

        verify(eventProcessor).invoke(oneKinesisEventInstance.data)
        verify(eventProcessor).invoke(anotherKinesisEventInstance.data)
    }

    @Test
    fun `should retry processing on exception`() {
        val (kinesisEvent, kinesisEventInstance) = eventPair()
        whenever(objectMapper.readValue(kinesisEvent, FooCreatedKinesisEvent::class.java)).thenReturn(kinesisEventInstance)
        whenever(configuration.maxRetries).thenReturn(2)
        whenever(eventProcessor.invoke(kinesisEventInstance.data))
                .doThrow(RuntimeException::class)
                .thenReturn(Unit) // stop throwing

        unit.processRecords(wrap(kinesisEvent))

        verify(eventProcessor, times(2)).invoke(kinesisEventInstance.data)
    }

    @Test
    fun `should checkpoint after processing event batch`() {
        val (kinesisEvent, kinesisEventInstance) = eventPair()
        whenever(objectMapper.readValue(kinesisEvent, FooCreatedKinesisEvent::class.java)).thenReturn(kinesisEventInstance)

        unit.processRecords(wrap(kinesisEvent))

        verify(streamCheckpointer).checkpoint()
    }

    //
    @Test
    fun `should retry checkpointing on dependency exception`() {
        val (kinesisEvent, kinesisEventInstance) = eventPair()
        whenever(objectMapper.readValue(kinesisEvent, FooCreatedKinesisEvent::class.java)).thenReturn(kinesisEventInstance)
        whenever(configuration.maxRetries).thenReturn(2)
        whenever(streamCheckpointer.checkpoint())
                .doThrow(KinesisClientLibDependencyException::class).then { } // stop throwing

        unit.processRecords(wrap(kinesisEvent))

        verify(streamCheckpointer, times(2)).checkpoint()
    }

    @Test
    fun `should retry checkpointing on throttling exception`() {
        val (kinesisEvent, kinesisEventInstance) = eventPair()
        whenever(objectMapper.readValue(kinesisEvent, FooCreatedKinesisEvent::class.java)).thenReturn(kinesisEventInstance)
        whenever(configuration.maxRetries).thenReturn(2)
        whenever(streamCheckpointer.checkpoint())
                .doThrow(ThrottlingException::class).then { } // stop throwing

        unit.processRecords(wrap(kinesisEvent))

        verify(streamCheckpointer, times(2)).checkpoint()
    }

    @Test
    fun `shouldn't retry checkpointing when application is shutting down`() {
        val (kinesisEvent, kinesisEventInstance) = eventPair()
        whenever(objectMapper.readValue(kinesisEvent, FooCreatedKinesisEvent::class.java)).thenReturn(kinesisEventInstance)
        whenever(configuration.maxRetries).thenReturn(2)
        whenever(streamCheckpointer.checkpoint())
                .doThrow(ShutdownException::class).then { } // stop throwing

        unit.processRecords(wrap(kinesisEvent))

        verify(streamCheckpointer).checkpoint()
    }

    @Test
    fun `shouldn't retry checkpointing on invalid state`() {
        val (kinesisEvent, kinesisEventInstance) = eventPair()
        whenever(objectMapper.readValue(kinesisEvent, FooCreatedKinesisEvent::class.java)).thenReturn(kinesisEventInstance)
        whenever(configuration.maxRetries).thenReturn(2)
        whenever(streamCheckpointer.checkpoint())
                .doThrow(InvalidStateException::class).then { } // stop throwing

        unit.processRecords(wrap(kinesisEvent))

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

    private fun eventPair() = Pair("""{"data":"{"name":"any-value"}"}""", FooCreatedKinesisEvent(data = FooCreatedEvent(Foo("any-value")), metadata = mock { }))
    private fun wrap(vararg kinesisEvents: String): ProcessRecordsInput {
        return mock {
            val eventRecords = kinesisEvents.toList().map { event -> mock<Record> { on { data } doReturn ByteBuffer.wrap(event.toByteArray()) } }
            on { records }.thenReturn(eventRecords)
            on { checkpointer }.thenReturn(streamCheckpointer)
        }
    }
}