package de.bringmeister.spring.aws.kinesis

import com.nhaarman.mockito_kotlin.argumentCaptor
import com.nhaarman.mockito_kotlin.verifyNoMoreInteractions
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.stereotype.Component
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import java.util.concurrent.atomic.AtomicBoolean

@Component
private class MyListener {

    private val invoked = AtomicBoolean(false)

    @KinesisListener("test-stream")
    fun handle(data: String, metadata: String) {
        invoked.set(true)
    }

    fun wasInvoked() = invoked.get()
}

@ActiveProfiles("test")
@RunWith(SpringRunner::class)
@SpringBootTest(
    classes = [
        JacksonConfiguration::class,
        AwsKinesisAutoConfiguration::class,
        MyListener::class
    ]
)
@MockBean(AwsKinesisInboundGateway::class)
class KinesisListenerPostProcessorIntegrationTest {

    @Autowired
    private lateinit var gateway: AwsKinesisInboundGateway

    @Autowired
    private lateinit var listener: MyListener

    @Test
    fun `should register listeners by default`() {

        val captor = argumentCaptor<KinesisInboundHandler<String, String>>()
        verify(gateway).register(captor.capture())

        assertThat(captor.allValues).hasSize(1)

        val proxy =  captor.firstValue
        assertThat(proxy.stream).isEqualTo("test-stream")
        assertThat(listener.wasInvoked()).isFalse()

        val record = Record("data", "meta")
        val context = TestKinesisInboundHandler.TestExecutionContext()
        proxy.handleRecord(record, context)
        assertThat(listener.wasInvoked()).isTrue()
    }
}

@ActiveProfiles("test")
@RunWith(SpringRunner::class)
@SpringBootTest(
    classes = [
        JacksonConfiguration::class,
        AwsKinesisAutoConfiguration::class,
        MyListener::class
    ],
    properties = ["aws.kinesis.listener.disabled=true"]
)
@MockBean(AwsKinesisInboundGateway::class)
class AwsKinesisAutoConfigurationDisabledListenerFactoryTest {

    @Autowired
    private lateinit var gateway: AwsKinesisInboundGateway

    @Test
    fun `should not proxy listeners when deactivated`() {
        verifyNoMoreInteractions(gateway)
    }
}
