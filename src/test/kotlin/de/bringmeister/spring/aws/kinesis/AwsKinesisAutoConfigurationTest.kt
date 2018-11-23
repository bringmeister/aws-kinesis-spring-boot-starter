package de.bringmeister.spring.aws.kinesis

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner

@SpringBootApplication
private class TestApplication

@ActiveProfiles("test")
@RunWith(SpringRunner::class)
@SpringBootTest(classes = [TestApplication::class])
class AwsKinesisAutoConfigurationTest {

    @Autowired(required = false)
    private lateinit var out: AwsKinesisOutboundGateway

    @Autowired(required = false)
    private lateinit var `in`: AwsKinesisInboundGateway

    @Test
    fun `should setup in- and outbound gateways`() {
        assertThat(out).isNotNull
        assertThat(`in`).isNotNull
    }
}

@RunWith(SpringRunner::class)
@SpringBootTest(
    classes = [TestApplication::class],
    properties = ["aws.kinesis.disabled=true"])
class AwsKinesisAutoConfigurationDisabledTest {

    @Autowired(required = false)
    private var out: AwsKinesisOutboundGateway? = null

    @Autowired(required = false)
    private var `in`: AwsKinesisInboundGateway? = null

    @Test
    fun `should not setup in- and outbound gateways when deactivated`() {
        assertThat(out).isNull()
        assertThat(`in`).isNull()
    }
}
