package de.bringmeister.spring.aws.kinesis.mdc

import de.bringmeister.spring.aws.kinesis.AwsKinesisAutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureBefore
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConditionalOnProperty("aws.kinesis.mdc.enabled", matchIfMissing = true)
@EnableConfigurationProperties(MdcSettings::class)
@AutoConfigureBefore(AwsKinesisAutoConfiguration::class)
class KinesisMdcAutoConfiguration {

    @Bean
    fun mdcPostProcessor(settings: MdcSettings) = MdcPostProcessor(settings)
}
