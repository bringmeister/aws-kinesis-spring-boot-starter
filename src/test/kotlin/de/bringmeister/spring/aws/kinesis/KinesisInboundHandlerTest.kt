package de.bringmeister.spring.aws.kinesis

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler.UnrecoverableException
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.Test

class KinesisInboundHandlerTest {

    private val handler = object : KinesisInboundHandler {
        override val stream get() = "test"
        override fun handleRecord(record: Record<*, *>, context: KinesisInboundHandler.ExecutionContext) { }
    }

    @Test
    fun `should not throw on ready`() {
        assertThatCode { handler.ready() }
            .doesNotThrowAnyException()
    }

    @Test
    fun `should not throw on shutdown`() {
        assertThatCode { handler.shutdown() }
            .doesNotThrowAnyException()
    }

    @Test
    fun `should wrap any exception in UnrecoverableException`() {

        val ex = MyException()
        assertThatCode { UnrecoverableException.unrecoverable<Unit> { throw ex } }
            .isInstanceOf(UnrecoverableException::class.java)
            .hasCause(ex)
    }

    class MyException : Exception("my expected exception")
}
