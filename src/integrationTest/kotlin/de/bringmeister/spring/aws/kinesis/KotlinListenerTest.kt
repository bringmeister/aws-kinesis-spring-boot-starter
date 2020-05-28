package de.bringmeister.spring.aws.kinesis

import org.assertj.core.api.Assertions.assertThat
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.test.context.junit4.SpringRunner
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@RunWith(SpringRunner::class)
@ContainerTest
@Import(KotlinListenerTest.Config::class)
class KotlinListenerTest {

    @Autowired
    lateinit var outbound: AwsKinesisOutboundGateway

    @TestConfiguration
    class Config {

        @Bean
        fun eventHandler() = object {

            @KinesisListener(STREAM)
            fun handle(data: String, metadata: String) {

                assertThat(data).isEqualTo(EXPECTED_DATA)
                assertThat(metadata).isEqualTo(EXPECTED_METADATA)

                expectedRecordsCounter.countDown()
            }
        }
    }

    companion object {
        @ClassRule
        @JvmField
        val kinesis = Containers.kinesis()

        @ClassRule
        @JvmField
        val dynamodb = Containers.dynamoDb()

        const val STREAM = "foo-event-stream"
        const val EXPECTED_DATA = "my-data"
        const val EXPECTED_METADATA = "my-metadata"

        lateinit var expectedRecordsCounter: CountDownLatch
    }

    @Test
    fun `should send and receive events`() {
        expectedRecordsCounter = CountDownLatch(1)
        outbound.send(STREAM, Record(EXPECTED_DATA, EXPECTED_METADATA))

        val recordsProcessed = waitForRecordsToBeProcessed()

        assertThat(recordsProcessed).isTrue()
    }

    private fun waitForRecordsToBeProcessed() = expectedRecordsCounter.await(1, TimeUnit.MINUTES)
}

