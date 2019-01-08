package de.bringmeister.spring.aws.kinesis.creation

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import de.bringmeister.spring.aws.kinesis.StreamInitializer
import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.TestKinesisOutboundStream
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class CreateStreamPostProcessorTest {

    private val mockStreamInitializer = mock<StreamInitializer> { }
    private val processor = CreateStreamPostProcessor(mockStreamInitializer)

    @Test
    fun `should create handler when initialized`() {
        val handler = TestKinesisInboundHandler()
        val processed = processor.postProcess(handler)

        assertThat(processed).isSameAs(handler)
        verify(mockStreamInitializer).createStreamIfMissing(handler.stream, 1)
    }

    @Test
    fun `should create stream when initialized`() {
        val stream = TestKinesisOutboundStream()
        val processed = processor.postProcess(stream)

        assertThat(processed).isSameAs(stream)
        verify(mockStreamInitializer).createStreamIfMissing(stream.stream, 1)
    }
}
