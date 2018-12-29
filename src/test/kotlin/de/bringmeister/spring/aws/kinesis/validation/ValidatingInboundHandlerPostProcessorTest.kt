package de.bringmeister.spring.aws.kinesis.creation

import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.validation.ValidatingInboundHandler
import de.bringmeister.spring.aws.kinesis.validation.ValidatingInboundHandlerPostProcessor
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import javax.validation.Validator

class ValidatingInboundHandlerPostProcessorTest {

    private val mockValidator = mock<Validator> { }
    private val handler = TestKinesisInboundHandler()

    @Test
    fun `should decorate handler with validating wrapper`() {
        val decorated = ValidatingInboundHandlerPostProcessor(mockValidator)
            .postProcess(handler)

        assertThat(decorated).isInstanceOf(ValidatingInboundHandler::class.java)
        assertThat(decorated.stream).isSameAs(handler.stream)
    }
}
