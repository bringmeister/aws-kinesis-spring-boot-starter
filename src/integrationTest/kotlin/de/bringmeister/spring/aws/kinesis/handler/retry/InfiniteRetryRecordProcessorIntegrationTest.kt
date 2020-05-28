package de.bringmeister.spring.aws.kinesis.handler.retry

import com.nhaarman.mockito_kotlin.doNothing
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.times
import com.nhaarman.mockito_kotlin.verify
import de.bringmeister.spring.aws.kinesis.AwsKinesisOutboundGateway
import de.bringmeister.spring.aws.kinesis.ContainerTest
import de.bringmeister.spring.aws.kinesis.Containers
import de.bringmeister.spring.aws.kinesis.KinesisListener
import de.bringmeister.spring.aws.kinesis.Record
import de.bringmeister.spring.aws.kinesis.retry.RetryableRecordProcessorSettings
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.util.concurrent.TimeUnit

@RunWith(SpringRunner::class)
@ContainerTest
@Import(InfiniteRetryRecordProcessorIntegrationTest.Config::class)
@TestPropertySource(properties = ["aws.kinesis.handler.retry.maxRetries=${RetryableRecordProcessorSettings.INFINITE_RETRIES}"])
class InfiniteRetryRecordProcessorIntegrationTest {

    companion object {
        @ClassRule
        @JvmField
        val kinesis = Containers.kinesis()

        @ClassRule
        @JvmField
        val dynamodb = Containers.dynamoDb()

        const val STREAM = "foo-event-stream"

        val processedRecords = mutableListOf<String>()
    }

    @TestConfiguration
    class Config {

        interface AnyDependency {
            fun call()
        }

        @Bean
        fun eventHandler(dependency: AnyDependency) = object {

            val log = LoggerFactory.getLogger(javaClass)

            @KinesisListener(STREAM)
            fun handle(data: String, metadata: String) {
                log.info("Processing [{}]", data)

                dependency.call() // might fail and should trigger retry

                processedRecords.add(data)
            }
        }

        // we want to provoke an exception within the record handler during processing of a record
        @Bean
        fun dependency() = mock<AnyDependency>()
    }

    @Autowired
    lateinit var outbound: AwsKinesisOutboundGateway

    @Autowired
    lateinit var mockDependency: Config.AnyDependency

    @Test
    fun `should retry infinitely until records are processed `() {
        val records = arrayOf(
            Record("1st", "any"),
            Record("2nd", "any"),
            Record("3rd", "any")
        )
        outbound.send(STREAM, *records)

        doNothing() // 1st call succeeds
            .doThrow(IllegalStateException("db is down")) // the following calls fail and should be retried until succeed...
            .doThrow(IllegalStateException("db is still down"))
            .doThrow(IllegalStateException("and still down...."))
            .doNothing() // call succeed again
            .`when`(mockDependency).call()

        await()
            .atMost(1, TimeUnit.MINUTES)
            .untilAsserted {
                assertThat(processedRecords).isEqualTo(listOf("1st", "2nd", "3rd"))
            }

        verify(mockDependency, times(records.size + 3)).call() // all records plus 3 retries
    }
}

