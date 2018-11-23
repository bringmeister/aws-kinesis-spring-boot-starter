package de.bringmeister.spring.aws.kinesis

import com.nhaarman.mockito_kotlin.argForWhich
import com.nhaarman.mockito_kotlin.argThat
import com.nhaarman.mockito_kotlin.argWhere
import com.nhaarman.mockito_kotlin.check
import com.nhaarman.mockito_kotlin.verifyNoMoreInteractions
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito.verify
import org.mockito.internal.util.MockUtil
import org.springframework.aop.support.AopUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.stereotype.Component
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner

@Component
private class MyListener {
    @KinesisListener("test-stream")
    fun handle(data: String, metadata: String) { }
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
class KinesisListenerPostProcessorTest {

    @Autowired
    private lateinit var gateway: AwsKinesisInboundGateway

    @Autowired
    private lateinit var listener: MyListener

    @Test
    fun `should register listeners by default`() {
        verify(gateway).register(check {
            assertThat(it.stream).isEqualTo("test-stream")
            assertThat(it.bean).isSameAs(listener)
        })
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
