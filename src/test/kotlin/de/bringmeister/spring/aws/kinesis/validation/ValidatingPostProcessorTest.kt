package de.bringmeister.spring.aws.kinesis.creation

import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.TestKinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.validation.ValidatingInboundHandler
import de.bringmeister.spring.aws.kinesis.validation.ValidatingOutboundStream
import de.bringmeister.spring.aws.kinesis.validation.ValidatingPostProcessor
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import javax.validation.Validator

class ValidatingPostProcessorTest {

    private val mockValidator = mock<Validator> { }
    private val processor = ValidatingPostProcessor(mockValidator)

    @Test
    fun `should decorate handler with validating wrapper`() {
        val handler = TestKinesisInboundHandler()
        val decorated = processor.postProcess(handler)

        assertThat(decorated).isInstanceOf(ValidatingInboundHandler::class.java)
        assertThat(decorated.stream).isSameAs(handler.stream)
    }

    @Test
    fun `should decorate stream with validating wrapper`() {
        val stream = TestKinesisOutboundStream()
        val decorated = processor.postProcess(stream)

        assertThat(decorated).isInstanceOf(ValidatingOutboundStream::class.java)
        assertThat(decorated.stream).isSameAs(stream.stream)
    }
}
