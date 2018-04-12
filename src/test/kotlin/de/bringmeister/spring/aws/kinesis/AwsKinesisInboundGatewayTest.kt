package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import org.junit.Test


class AwsKinesisInboundGatewayTest : AbstractTest() {

    val clientProvider: AwsKinesisClientProvider = mock {
        on { consumerConfig(any()) } doReturn mock<KinesisClientLibConfiguration> { }
    }

    val workerFactory: WorkerFactory = mock {
        on {
            worker(any(), any(), any<EventProcessor<FooCreatedEvent>>())
        } doReturn mock<Worker> { }
    }

    val unit = AwsKinesisInboundGateway(clientProvider, workerFactory)

    @Test
    fun `should get client configuration by stream name`() {
        unit.listen(FooCreatedKinesisEvent.STREAM_NAME, FooCreatedKinesisEvent::class.java, {})

        verify(clientProvider).consumerConfig(FooCreatedKinesisEvent.STREAM_NAME)
    }

    @Test
    fun `should get worker by client configuration`() {
        val processor: (FooCreatedEvent) -> Unit = {}
        val clientConfig: KinesisClientLibConfiguration = mock { }
        whenever(clientProvider.consumerConfig(FooCreatedKinesisEvent.STREAM_NAME)).thenReturn(clientConfig)

        unit.listen(FooCreatedKinesisEvent.STREAM_NAME, FooCreatedKinesisEvent::class.java, processor)

        verify(workerFactory).worker(eq(clientConfig), eq(FooCreatedKinesisEvent::class.java), eq(processor))
    }

    @Test
    fun `should run worker`() {
        val worker:Worker = mock {  }
        whenever(workerFactory.worker(any(), eq(FooCreatedKinesisEvent::class.java), any())).thenReturn(worker)

        unit.listen(FooCreatedKinesisEvent.STREAM_NAME, FooCreatedKinesisEvent::class.java) {}

        verify(worker).run()
    }
}