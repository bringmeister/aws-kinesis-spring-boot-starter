package de.bringmeister.spring.aws.kinesis

import de.bringmeister.spring.aws.kinesis.KinesisInboundHandler.UnrecoverableException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.Test

class KinesisInboundHandlerTest {

    private val handler = object : KinesisInboundHandler<Any, Any> {

        override val stream get() = "test"
        override fun handleRecord(record: Record<Any, Any>, context: KinesisInboundHandler.ExecutionContext) {}

        override fun dataType() = Any::class.java
        override fun metaType() = Any::class.java
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
    fun `should not throw on handleDeserializationError`() {
        val cause = RuntimeException("expected")
        val context = TestKinesisInboundHandler.TestExecutionContext()
        assertThatCode { handler.handleDeserializationError(cause, context) }
            .doesNotThrowAnyException()
    }

    @Test
    fun `should wrap any exception in UnrecoverableException`() {

        val ex = MyException()
        assertThatCode { UnrecoverableException.unrecoverable<Unit> { throw ex } }
            .isInstanceOf(UnrecoverableException::class.java)
            .hasCause(ex)
    }

    @Test
    fun `should return when no exception occurred`() {

        val ret = Any()
        assertThatCode {
            assertThat(UnrecoverableException.unrecoverable { ret }).isSameAs(ret)
        }
            .doesNotThrowAnyException()
    }

    class MyException : Exception("my expected exception")
}
