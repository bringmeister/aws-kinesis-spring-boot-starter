package de.bringmeister.spring.aws.kinesis

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.verifyNoMoreInteractions
import com.nhaarman.mockito_kotlin.whenever
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class KinesisListenerPostProcessorTest {

    private val bean = Any()

    private val mockHandler = mock<KinesisListenerProxy> { }
    private val mockGateway = mock<AwsKinesisInboundGateway> { }
    private val mockFactory = mock<KinesisListenerProxyFactory> { }

    @Test
    fun `should register annotated methods at the gateway`() {

        whenever(mockFactory.proxiesFor(bean)).thenReturn(listOf(mockHandler))
        val processor = KinesisListenerPostProcessor(mockGateway, mockFactory)

        val post = processor.postProcessAfterInitialization(bean, "test")

        assertThat(post).isSameAs(bean)
        verify(mockGateway).register(mockHandler)
    }

    @Test
    fun `should apply post processors to registered handlers`() {

        whenever(mockFactory.proxiesFor(bean)).thenReturn(listOf(mockHandler))
        val decorated = TestKinesisInboundHandler()
        val processor = KinesisListenerPostProcessor(mockGateway, mockFactory, listOf(TestPostProcessor(decorated)))

        val post = processor.postProcessAfterInitialization(bean, "test")

        assertThat(post).isSameAs(bean)
        verify(mockGateway).register(decorated)
    }

    @Test
    fun `should do nothing when no handlers are created`() {

        whenever(mockFactory.proxiesFor(bean)).thenReturn(emptyList())
        val processor = KinesisListenerPostProcessor(mockGateway, mockFactory)

        val post = processor.postProcessAfterInitialization(bean, "test")

        assertThat(post).isSameAs(bean)
        verifyNoMoreInteractions(mockGateway)
    }

    private class TestPostProcessor(
        private val decorated: KinesisInboundHandler
    ): KinesisInboundHandlerPostProcessor {
        override fun postProcess(handler: KinesisInboundHandler) = decorated
    }
}
