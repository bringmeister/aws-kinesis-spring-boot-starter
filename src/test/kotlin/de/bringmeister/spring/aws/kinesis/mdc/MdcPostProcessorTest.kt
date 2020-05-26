package de.bringmeister.spring.aws.kinesis.mdc

import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.TestKinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.validation.ValidatingInboundHandler
import de.bringmeister.spring.aws.kinesis.validation.ValidatingOutboundStream
import de.bringmeister.spring.aws.kinesis.validation.ValidatingPostProcessor
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import javax.validation.Validator

class MdcPostProcessorTest {

    private val processor = MdcPostProcessor(MdcSettings())

    @Test
    fun `should decorate handler with mdc wrapper`() {
        val handler = TestKinesisInboundHandler()
        val decorated = processor.postProcess(handler)

        assertThat(decorated).isInstanceOf(MdcInboundHandler::class.java)
        assertThat(decorated.stream).isSameAs(handler.stream)
    }
}
