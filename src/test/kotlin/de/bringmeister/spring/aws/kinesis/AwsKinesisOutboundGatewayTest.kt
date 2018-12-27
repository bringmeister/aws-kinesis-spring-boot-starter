package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.PutRecordsRequest
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.anyVararg
import com.nhaarman.mockito_kotlin.argumentCaptor
import com.nhaarman.mockito_kotlin.eq
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.verifyZeroInteractions
import com.nhaarman.mockito_kotlin.whenever
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import javax.validation.ValidationException
import javax.validation.Validator

class AwsKinesisOutboundGatewayTest {

    val requestFactory = mock<RequestFactory> { }
    val clientProvider = mock<KinesisClientProvider> { }
    val streamInitializer = mock<StreamInitializer>()
    val validator = mock<Validator>()
    val outboundGateway = AwsKinesisOutboundGateway(
        clientProvider,
        requestFactory,
        streamInitializer,
        validator
    )

    @Test
    fun `should create and send kinesis request`() {
        val request = mock<PutRecordsRequest> { }
        val producer = mock<AmazonKinesis> { }

        val streamName = "some-stream-name"
        whenever(requestFactory.request(eq(streamName), any<Record<FooCreatedEvent, EventMetadata>>())).thenReturn(
            request
        )
        whenever(clientProvider.clientFor(streamName)).thenReturn(producer)
        whenever(producer.putRecords(any())).thenReturn(mock { })

        val event = FooCreatedEvent("any-value")
        val metadata = mock<EventMetadata> { }
        outboundGateway.send(streamName, Record(event, metadata))

        val stringCaptor = argumentCaptor<String>()
        val dataCaptor = argumentCaptor<Record<FooCreatedEvent, EventMetadata>>()
        verify(requestFactory).request(stringCaptor.capture(), dataCaptor.capture())
        assertThat(stringCaptor.firstValue).isEqualTo(streamName)
        assertThat(dataCaptor.firstValue.data).isEqualTo(event)
        assertThat(dataCaptor.firstValue.metadata).isEqualTo(metadata)
        verify(clientProvider).clientFor(streamName)
        verify(producer).putRecords(request)
    }

    @Test(expected = ValidationException::class)
    fun `should reject invalid event`() {
        val streamName = "some-stream-name"
        val invalidEvent = FooCreatedEvent(foo = "")

        whenever(validator.validate(anyVararg<FooCreatedEvent>())).thenReturn(setOf(mock()))
        verifyZeroInteractions(requestFactory)
        verifyZeroInteractions(clientProvider)

        outboundGateway.send(streamName, Record(invalidEvent, mock<EventMetadata> { }))
    }

    @Test
    fun `should accept invalid event when no validator passed`() {
        val streamName = "some-stream-name"
        val invalidEvent = FooCreatedEvent(foo = "")
        val request = mock<PutRecordsRequest> { }
        val producer = mock<AmazonKinesis> { }
        val outboundGatewayWithoutValidator =
            AwsKinesisOutboundGateway(
                clientProvider,
                requestFactory,
                streamInitializer
            )
        whenever(requestFactory.request(eq(streamName), any<Record<FooCreatedEvent, EventMetadata>>())).thenReturn(
            request
        )
        whenever(clientProvider.clientFor(streamName)).thenReturn(producer)
        whenever(producer.putRecords(any())).thenReturn(mock { })

        outboundGatewayWithoutValidator.send(streamName, Record(invalidEvent, mock<EventMetadata> { }))

        verify(producer).putRecords(request)
    }
}
