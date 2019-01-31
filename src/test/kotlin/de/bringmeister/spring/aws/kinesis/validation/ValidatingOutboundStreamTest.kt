package de.bringmeister.spring.aws.kinesis.validation

import com.nhaarman.mockito_kotlin.anyVararg
import com.nhaarman.mockito_kotlin.argWhere
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.verifyZeroInteractions
import com.nhaarman.mockito_kotlin.whenever
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStream
import de.bringmeister.spring.aws.kinesis.Record
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.Test
import javax.validation.ValidationException
import javax.validation.Validator

class ValidatingOutboundStreamTest {

    private val mockDelegate = mock<KinesisOutboundStream> { }
    private val mockValidator = mock<Validator> { }
    private val handler = ValidatingOutboundStream(mockDelegate, mockValidator)

    private val data = Any()
    private val meta = Any()
    private val record = Record(data, meta)

    @Test
    fun `should not invoke delegate on invalid record`() {
        assertThatCode {
            whenever(mockValidator.validate(anyVararg<Any>())).thenReturn(setOf(mock()))
            handler.send(record)
        }.isInstanceOf(ValidationException::class.java)
        verifyZeroInteractions(mockDelegate)
    }

    @Test
    fun `should invoke delegate on valid record`() {
        whenever(mockValidator.validate(anyVararg<Any>())).thenReturn(emptySet())
        handler.send(record)
        verify(mockDelegate).send(argWhere { it == record })
    }
}
