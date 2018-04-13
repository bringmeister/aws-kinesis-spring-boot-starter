package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.type.TypeFactory
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
            worker(any(), any<EventHandler<FooCreatedEvent, EventMetadata>>())
        } doReturn mock<Worker> { }
    }

    val eventType = mock<JavaType> { }
    val typeFactory: TypeFactory = mock {
        on { constructParametricType(com.nhaarman.mockito_kotlin.any(), com.nhaarman.mockito_kotlin.any<Class<*>>()) } doReturn eventType
    }

    val objectMapper = mock<ObjectMapper> {
        on { typeFactory } doReturn typeFactory
    }


    val unit = AwsKinesisInboundGateway(objectMapper, clientProvider, workerFactory)

    @Test
    fun `should get client configuration by stream name`() {
        val eventHandler = { _: FooCreatedEvent, _: EventMetadata -> }
        unit.listen("foo-stream", eventHandler, FooCreatedEvent::class.java, EventMetadata::class.java)

        verify(clientProvider).consumerConfig("foo-stream")
    }

    @Test
    fun `should get worker by client configuration`() {
        val eventHandler = { _: FooCreatedEvent, _: EventMetadata -> }
        val clientConfig: KinesisClientLibConfiguration = mock { }
        whenever(clientProvider.consumerConfig("foo-stream")).thenReturn(clientConfig)

        unit.listen("foo-stream", eventHandler, FooCreatedEvent::class.java, EventMetadata::class.java)

        verify(workerFactory).worker(eq(clientConfig), eq(EventHandler("foo-stream", eventType, eventHandler)))
    }

    @Test
    fun `should run worker`() {
        val worker: Worker = mock { }
        whenever(workerFactory.worker(any(), any<EventHandler<*, *>>())).thenReturn(worker)

        unit.listen("foo-stream", { _: FooCreatedEvent, _: EventMetadata -> }, FooCreatedEvent::class.java, EventMetadata::class.java)

        verify(worker).run()
    }
}