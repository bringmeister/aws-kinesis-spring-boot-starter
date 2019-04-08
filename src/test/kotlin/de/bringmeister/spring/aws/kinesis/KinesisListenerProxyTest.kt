package de.bringmeister.spring.aws.kinesis

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.Test

class KinesisListenerProxyTest {

    @Test
    fun `should fail fast when method signature is unsupported`() {

        MyListener::class.java.methods
            .filter { it.isAnnotationPresent(KinesisListener::class.java) }
            .forEach {
                val exception = catchThrowable {
                    KinesisListenerProxy(
                        bean = MyListener,
                        method = it,
                        stream = "test-${it.name}"
                    )
                }
                assertThat(exception).isInstanceOf(UnsupportedOperationException::class.java)
            }
    }

    private object MyListener {

        @KinesisListener(stream = "stream-1")
        fun noArg() {
            // empty
        }

        @KinesisListener(stream = "stream-2")
        fun tooManyArgs(data: FooCreatedEvent, metadata: EventMetadata, unsupported: Any) {
            // empty
        }
    }
}
