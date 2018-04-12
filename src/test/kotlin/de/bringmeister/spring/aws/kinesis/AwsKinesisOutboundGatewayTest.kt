package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.fasterxml.jackson.databind.ObjectMapper
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import org.junit.Test

class AwsKinesisOutboundGatewayTest {

    val requestFactory = mock<RequestFactory> { }
    val clientProvider = mock<AwsKinesisClientProvider> { }
    val properties = mock<AwsKinesisSettings> {
        val producerSettings = mock<ProducerSettings> {
            on { streamName }.thenReturn(FooCreatedKinesisEvent.STREAM_NAME)
        }

        on { producer }.thenReturn(mutableListOf(producerSettings))
    }

    val unit = AwsKinesisOutboundGateway(properties, clientProvider, requestFactory)

    @Test
    fun `should create and send kinesis request`() {
        val event = FooCreatedKinesisEvent(data = FooCreatedEvent(Foo("any-value")), metadata = mock { })
        val request = mock<PutRecordRequest> { }
        val producer = mock<AmazonKinesis> { }

        whenever(requestFactory.request(eq(event))).thenReturn(request)
        whenever(clientProvider.producer(FooCreatedKinesisEvent.STREAM_NAME)).thenReturn(producer)
        whenever(producer.putRecord(any())).thenReturn(mock { })

        unit.initKinesisClients()
        unit.send(event)

        verify(requestFactory).request(event)
        verify(clientProvider).producer(FooCreatedKinesisEvent.STREAM_NAME)
        verify(producer).putRecord(request)
    }
}

class RequestFactoryTest {

    val objectMapper = mock<ObjectMapper> {
        on { writeValueAsBytes(any()) }.thenReturn("any-string".toByteArray())
    }

    val unit = RequestFactory(objectMapper)

    @Test
    fun `should use event stream name for request`() {
        val request = unit.request(FooCreatedKinesisEvent(data = FooCreatedEvent(Foo("any-value")), metadata = mock { }))

        assertThat(request.streamName, equalTo(FooCreatedKinesisEvent.STREAM_NAME))
    }

    @Test
    fun `should serialize event using object mapper`() {
        val event = FooCreatedKinesisEvent(data = FooCreatedEvent(Foo("any-value")), metadata = mock { })

        val request = unit.request(event)

        verify(objectMapper).writeValueAsBytes(event)
        assertThat(request.streamName, equalTo(FooCreatedKinesisEvent.STREAM_NAME))
    }
}