package de.bringmeister.spring.aws.kinesis.creation

import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.TestKinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.validation.ValidatingOutboundStream
import de.bringmeister.spring.aws.kinesis.validation.ValidatingOutboundStreamPostProcessor
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import javax.validation.Validator

class ValidatingOutboundStreamPostProcessorTest {

    private val mockValidator = mock<Validator> { }
    private val stream = TestKinesisOutboundStream()

    @Test
    fun `should decorate handler with validating wrapper`() {
        val decorated = ValidatingOutboundStreamPostProcessor(mockValidator)
            .postProcess(stream)

        assertThat(decorated).isInstanceOf(ValidatingOutboundStream::class.java)
        assertThat(decorated.stream).isSameAs(stream.stream)
    }
}
