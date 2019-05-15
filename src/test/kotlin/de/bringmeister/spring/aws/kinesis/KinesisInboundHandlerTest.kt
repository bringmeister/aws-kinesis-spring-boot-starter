package de.bringmeister.spring.aws.kinesis

import org.assertj.core.api.Assertions.assertThatCode
import org.junit.Test
import java.nio.ByteBuffer

class KinesisInboundHandlerTest {

    private val handler = object : KinesisInboundHandler<Any, Any> {

        override val stream get() = "test"
        override fun handleRecord(record: Record<Any, Any>, context: KinesisInboundHandler.ExecutionContext) {}
        override fun handleRecords(records: List<Record<Any, Any>>, context: KinesisInboundHandler.ExecutionContext) {}

        override fun dataType() = Any::class.java
        override fun metaType() = Any::class.java
        override fun isBatch() = false
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
        assertThatCode { handler.handleDeserializationError(cause, ByteBuffer.allocate(0), context) }
            .doesNotThrowAnyException()
    }
}
