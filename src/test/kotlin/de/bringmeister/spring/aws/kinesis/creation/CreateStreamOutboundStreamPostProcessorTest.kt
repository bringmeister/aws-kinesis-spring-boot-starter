package de.bringmeister.spring.aws.kinesis.creation

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import de.bringmeister.spring.aws.kinesis.StreamInitializer
import de.bringmeister.spring.aws.kinesis.TestKinesisOutboundStream
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class CreateStreamOutboundStreamPostProcessorTest {

    private val mockStreamInitializer = mock<StreamInitializer> { }
    private val stream = TestKinesisOutboundStream()

    @Test
    fun `should create stream when initialized`() {
        val stream = CreateStreamOutboundStreamPostProcessor(mockStreamInitializer)
            .postProcess(stream)

        assertThat(stream).isSameAs(this.stream)
        verify(mockStreamInitializer).createStreamIfMissing(stream.stream, 1)
    }
}
