package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.AmazonKinesis
import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verifyNoMoreInteractions
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class AwsKinesisOutboundStreamFactoryTest {

    private val mockClient = mock<AmazonKinesis> { }
    private val mockClientProvider = mock<KinesisClientProvider> {
        on { clientFor(any()) } doReturn mockClient
    }
    private val mockRequestFactory = mock<RequestFactory> { }

    @Test
    fun `should register annotated methods at the gateway`() {

        val factory = AwsKinesisOutboundStreamFactory(mockClientProvider, mockRequestFactory)

        val stream = factory.forStream("test")

        assertThat(stream).isInstanceOf(AwsKinesisOutboundStream::class.java)
        assertThat(stream.stream).isEqualTo("test")
        verifyNoMoreInteractions(mockClient, mockRequestFactory)
    }

    @Test
    fun `should apply post processors to registered handlers`() {

        val decorated = TestKinesisOutboundStream()
        val factory = AwsKinesisOutboundStreamFactory(
            mockClientProvider,
            mockRequestFactory,
            listOf(TestPostProcessor(decorated))
        )

        val stream = factory.forStream("test")

        assertThat(stream).isSameAs(decorated)
        verifyNoMoreInteractions(mockClient, mockRequestFactory)
    }

    private class TestPostProcessor(
        private val decorated: KinesisOutboundStream
    ) : KinesisOutboundStreamPostProcessor {
        override fun postProcess(stream: KinesisOutboundStream) = decorated
    }
}
