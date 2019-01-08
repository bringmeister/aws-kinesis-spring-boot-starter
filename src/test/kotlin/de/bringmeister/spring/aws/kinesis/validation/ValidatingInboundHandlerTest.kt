package de.bringmeister.spring.aws.kinesis.validation

import com.nhaarman.mockito_kotlin.anyVararg
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.verifyZeroInteractions
import com.nhaarman.mockito_kotlin.whenever
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler
import de.bringmeister.spring.aws.kinesis.Record
import de.bringmeister.spring.aws.kinesis.TestKinesisInboundHandler
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.Test
import javax.validation.ValidationException
import javax.validation.Validator

class ValidatingInboundHandlerTest {

    private val mockDelegate = mock<KinesisInboundHandler<Any, Any>> { }
    private val mockValidator = mock<Validator> { }
    private val handler = ValidatingInboundHandler(mockDelegate, mockValidator)

    private val record = Record(Any(), Any())
    private val context = TestKinesisInboundHandler.TestExecutionContext()

    @Test
    fun `should not invoke delegate on invalid record`() {
        assertThatCode {
                whenever(mockValidator.validate(anyVararg<Any>())).thenReturn(setOf(mock()))
                handler.handleRecord(record, context)
            }
            .hasCauseInstanceOf(ValidationException::class.java)
            .isInstanceOf(KinesisInboundHandler.UnrecoverableException::class.java)
        verifyZeroInteractions(mockDelegate)
    }

    @Test
    fun `should invoke delegate listener on valid record`() {
        whenever(mockValidator.validate(anyVararg<Any>())).thenReturn(emptySet())
        handler.handleRecord(record, context)
        verify(mockDelegate).handleRecord(record, context)
    }
}
