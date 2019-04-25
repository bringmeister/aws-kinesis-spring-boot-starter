package de.bringmeister.spring.aws.kinesis.retry

import de.bringmeister.spring.aws.kinesis.RetrySettings
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.validation.annotation.Validated
import java.time.Duration
import javax.validation.constraints.Min

@Configuration
@EnableConfigurationProperties(RetryableRecordProcessorSettings::class)
class RetryableRecordHandlerAutoConfiguration {

    @Bean
    fun retryableRecordPostProcessor(settings: RetryableRecordProcessorSettings) = RetryableRecordHandlerPostProcessor(settings)
}

@Validated
@ConfigurationProperties(prefix = "aws.kinesis.handler.retry")
class RetryableRecordProcessorSettings {

    companion object {
        const val INFINITE_RETRIES = -1
    }

    @Min(-1)
    var maxRetries = RetrySettings.NO_RETRIES
    var backoff = Duration.ofSeconds(1)
}