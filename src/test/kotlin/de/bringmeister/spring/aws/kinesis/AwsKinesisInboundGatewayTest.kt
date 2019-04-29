package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import org.junit.Test

class AwsKinesisInboundGatewayTest {

    val workerStarter: WorkerStarter = mock { }
    val worker = mock<Worker> { }
    private val handler = TestKinesisInboundHandler()
    val mockRecordDeserializer = mock<RecordDeserializer<Any, Any>> { }
    val mockRecordDeserializerFactory = mock<RecordDeserializerFactory> {
        on { deserializerFor(handler) } doReturn mockRecordDeserializer
    }
    val workerFactory: WorkerFactory = mock {
        on { worker(handler, mockRecordDeserializer) } doReturn worker
    }

    val inboundGateway = AwsKinesisInboundGateway(workerFactory, workerStarter, mockRecordDeserializerFactory)

    @Test
    fun `when registering a listener instance it should create worker`() {
        inboundGateway.register(handler)
        verify(workerFactory).worker(handler, mockRecordDeserializer)
    }

    @Test
    fun `when registering a listener instance it should run worker`() {
        inboundGateway.register(handler)
        verify(workerStarter).startWorker(handler.stream, worker)
    }

    @Test
    fun `should apply post processors to registered handlers`() {

        whenever(mockRecordDeserializerFactory.deserializerFor<Any, Any>(any())).thenReturn(mockRecordDeserializer)
        whenever(workerFactory.worker(any(), eq(mockRecordDeserializer))).thenReturn(worker)

        val decorated = TestKinesisInboundHandler()
        val inboundGateway = AwsKinesisInboundGateway(
            workerFactory,
            workerStarter,
            mockRecordDeserializerFactory,
            listOf(TestPostProcessor(decorated))
        )

        inboundGateway.register(handler)

        verify(mockRecordDeserializerFactory).deserializerFor(decorated)
        verify(workerFactory).worker(decorated, mockRecordDeserializer)
    }

    private class TestPostProcessor(
        private val decorated: KinesisInboundHandler<*, *>
    ): KinesisInboundHandlerPostProcessor {
        override fun postProcess(handler: KinesisInboundHandler<*, *>) = decorated
    }
}
