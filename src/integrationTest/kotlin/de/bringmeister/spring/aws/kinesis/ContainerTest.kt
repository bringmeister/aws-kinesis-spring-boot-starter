package de.bringmeister.spring.aws.kinesis

import de.bringmeister.spring.aws.kinesis.creation.KinesisCreateStreamAutoConfiguration
import de.bringmeister.spring.aws.kinesis.metrics.KinesisMetricsAutoConfiguration
import de.bringmeister.spring.aws.kinesis.retry.RetryableRecordHandlerAutoConfiguration
import de.bringmeister.spring.aws.kinesis.validation.KinesisValidationAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles("kinesis-local")
@SpringBootTest(
    classes = [
        JacksonConfiguration::class,
        KinesisLocalConfiguration::class,
        AwsKinesisAutoConfiguration::class,
        RetryableRecordHandlerAutoConfiguration::class,
        KinesisCreateStreamAutoConfiguration::class,
        KinesisMetricsAutoConfiguration::class,
        KinesisValidationAutoConfiguration::class
    ]
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
annotation class ContainerTest
