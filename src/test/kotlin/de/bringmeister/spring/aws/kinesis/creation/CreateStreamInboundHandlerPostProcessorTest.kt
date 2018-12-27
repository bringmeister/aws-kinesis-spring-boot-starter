package de.bringmeister.spring.aws.kinesis.creation

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import de.bringmeister.spring.aws.kinesis.StreamInitializer
import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

class CreateStreamInboundHandlerPostProcessorTest {

    private val mockStreamInitializer = mock<StreamInitializer> { }
    private val handler = TestKinesisInboundHandler()

    @Test
    fun `should create stream when initialized`() {
        val handler = CreateStreamInboundHandlerPostProcessor(mockStreamInitializer)
            .postProcess(handler)

        assertThat(handler).isSameAs(this.handler)
        verify(mockStreamInitializer).createStreamIfMissing(handler.stream, 1)
    }
}
