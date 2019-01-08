package de.bringmeister.spring.aws.kinesis.creation

import de.bringmeister.spring.aws.kinesis.AwsKinesisAutoConfiguration
import de.bringmeister.spring.aws.kinesis.StreamInitializer
import org.springframework.boot.autoconfigure.AutoConfigureBefore
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConditionalOnProperty("aws.kinesis.create-streams")
@AutoConfigureBefore(AwsKinesisAutoConfiguration::class)
class KinesisCreateStreamAutoConfiguration(
    private val streamInitializer: StreamInitializer
) {

    @Bean
    fun createStreamPostProcessor() = CreateStreamPostProcessor(streamInitializer)
}
