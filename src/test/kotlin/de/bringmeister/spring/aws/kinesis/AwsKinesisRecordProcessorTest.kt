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
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.doThrow
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.Before
import org.junit.Test
import org.springframework.context.ApplicationEventPublisher
import java.nio.ByteBuffer

class AwsKinesisRecordProcessorTest {

    val messageJson = """{"data":{"foo":"any-field"},"metadata":{"sender":"test"}}"""
    val mapper = ObjectMapper().registerModule(KotlinModule())
    val streamCheckpointer = mock<IRecordProcessorCheckpointer> {}
    val configuration = RecordProcessorConfiguration(2, 1)
    var handlerMock = mock<(FooCreatedEvent, EventMetadata) -> Unit> { }
    var applicationEventPublisher = mock<ApplicationEventPublisher> {
        on { publishEvent(any()) }.then { }
    }

    var handler = object {

        @KinesisListener(stream = "foo-event-stream")
        fun handle(data: FooCreatedEvent, metadata: EventMetadata) {
            handlerMock.invoke(data, metadata)
        }
    }

    val kinesisListener = KinesisListenerProxyFactory(AopProxyUtils()).proxiesFor(handler)[0]
    private val kinesisListenerCapture = KinesisListenerCapture(kinesisListener)

    val recordDeserializer = ObjectMapperRecordDeserializerFactory(mapper)
        .deserializerFor(kinesisListenerCapture)

    val recordProcessor =
        AwsKinesisRecordProcessor(recordDeserializer, configuration, kinesisListenerCapture, applicationEventPublisher)

    @Before
    fun setUp() {

        val initializationInput = mock<InitializationInput> {
            on { shardId }.thenReturn("any-shard")
        }

        recordProcessor.initialize(initializationInput)
    }

    @Test
    fun `should invoke Kinesis listener for each record`() {

        val record1 = wrap(messageJson)
        val record2 = wrap(messageJson)

        recordProcessor.processRecords(record1)
        recordProcessor.processRecords(record2)

        verify(handlerMock, times(2)).invoke(FooCreatedEvent("any-field"), EventMetadata("test"))
        verify(streamCheckpointer, times(2)).checkpoint()
    }

    @Test
    fun `should retry processing on exception`() {

        whenever(handlerMock.invoke(any(), any()))
            .doThrow(RuntimeException::class)
            .then { } // stop throwing

        val record = wrap(messageJson)
        recordProcessor.processRecords(record)

        verify(handlerMock, times(2)).invoke(any(), any()) // handler fails, so it's retried 2 times
        verify(streamCheckpointer).checkpoint() // however, we checkpoint only once after success

        val contexts = kinesisListenerCapture.contexts()
        assertThat(contexts[0].isRetry).isFalse()
        assertThat(contexts[1].isRetry).isTrue()
    }

    @Test
    fun `should not retry processing on UnrecoverableException`() {

        whenever(handlerMock.invoke(any(), any()))
            .doThrow(KinesisInboundHandler.UnrecoverableException(RuntimeException()))

        val record = wrap(messageJson)
        recordProcessor.processRecords(record)

        verify(handlerMock).invoke(any(), any()) // handler fails hard without retry
        verify(streamCheckpointer).checkpoint() // we checkpoint once failed
    }

    @Test
    fun `should not bubble exception when all retries fail`() {

        whenever(handlerMock.invoke(any(), any())).doThrow(RuntimeException::class)

        val record = wrap(messageJson)
        assertThatCode { recordProcessor.processRecords(record) }
            .doesNotThrowAnyException()
    }

    @Test
    fun `should not bubble exception when deserialization fails`() {

        val record = wrap("invalid")
        assertThatCode { recordProcessor.processRecords(record) }
            .doesNotThrowAnyException()
    }

    @Test
    fun `should retry checkpointing on KinesisClientLibDependencyException`() {

        whenever(streamCheckpointer.checkpoint())
            .doThrow(KinesisClientLibDependencyException::class)
            .then { } // stop throwing

        val record = wrap(messageJson)
        recordProcessor.processRecords(record)

        verify(handlerMock).invoke(FooCreatedEvent("any-field"), EventMetadata("test")) // handler is successful
        verify(streamCheckpointer, times(2)).checkpoint() // but checkpointing fails once and is tried 2 times
    }

    @Test
    fun `should retry checkpointing on ThrottlingException`() {

        whenever(streamCheckpointer.checkpoint())
            .doThrow(ThrottlingException::class)
            .then { } // stop throwing

        val record = wrap(messageJson)
        recordProcessor.processRecords(record)

        verify(handlerMock).invoke(FooCreatedEvent("any-field"), EventMetadata("test")) // handler is successful
        verify(streamCheckpointer, times(2)).checkpoint() // but checkpointing fails once and is tried 2 times
    }

    @Test
    fun `should not bubble exception when checkpointing on ThrottlingException runs out of retries`() {

        whenever(streamCheckpointer.checkpoint()).doThrow(ThrottlingException::class)

        val record = wrap(messageJson)
        assertThatCode { recordProcessor.processRecords(record) }
            .doesNotThrowAnyException()
    }

    @Test
    fun `shouldn't retry checkpointing on ShutdownException`() {

        whenever(streamCheckpointer.checkpoint())
            .doThrow(ShutdownException::class) // stop throwing

        val record = wrap(messageJson)
        recordProcessor.processRecords(record)

        verify(streamCheckpointer).checkpoint()
    }

    @Test
    fun `shouldn't retry checkpointing on InvalidStateException`() {

        whenever(streamCheckpointer.checkpoint())
            .doThrow(InvalidStateException::class) // stop throwing

        val record = wrap(messageJson)
        recordProcessor.processRecords(record)

        verify(streamCheckpointer).checkpoint()
    }

    @Test
    fun `should checkpoint on resharding`() {
        val shutdownInput = mock<ShutdownInput> {
            on { shutdownReason } doReturn ShutdownReason.TERMINATE // re-sharding
            on { checkpointer } doReturn streamCheckpointer
        }
        recordProcessor.shutdown(shutdownInput)

        verify(streamCheckpointer).checkpoint()
    }

    private fun wrap(vararg recordJsons: String): ProcessRecordsInput {
        val records = recordJsons.toList().map { record -> Record().withData(ByteBuffer.wrap(record.toByteArray())) }
        return ProcessRecordsInput()
            .withRecords(records)
            .withCheckpointer(streamCheckpointer)
    }

    private class KinesisListenerCapture(
        private val kinesisListener: KinesisListenerProxy
    ) : KinesisInboundHandler<Any, Any> by kinesisListener {

        private val contexts = mutableListOf<KinesisInboundHandler.ExecutionContext>()

        override fun handleRecord(
            record: de.bringmeister.spring.aws.kinesis.Record<Any, Any>,
            context: KinesisInboundHandler.ExecutionContext
        ) {
            contexts.add(context)
            kinesisListener.handleRecord(record, context)
        }

        fun contexts(): List<KinesisInboundHandler.ExecutionContext> {
            val list = contexts.toList()
            contexts.clear()
            return list
        }
    }
}
