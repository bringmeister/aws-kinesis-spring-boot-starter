package de.bringmeister.spring.aws.kinesis.health

import com.nhaarman.mockito_kotlin.mock
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.boot.actuate.health.Status

class KinesisStreamHealthIndicatorTest {

    @Test
    fun `should indicate DOWN on error`() {
        val indicator = KinesisStreamHealthIndicator(
            kinesis = mock { },
            stream = "test",
            useSummaryApi = true,
            ignoreNotFoundUntilCreated = false
        )
        val health = indicator.health()

        assertThat(health.status).isEqualTo(Status.DOWN)
    }
}
