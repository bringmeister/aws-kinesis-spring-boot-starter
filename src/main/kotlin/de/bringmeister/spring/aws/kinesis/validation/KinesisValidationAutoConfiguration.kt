package de.bringmeister.spring.aws.kinesis.validation

import de.bringmeister.spring.aws.kinesis.AwsKinesisAutoConfiguration
import de.bringmeister.spring.aws.kinesis.KinesisInboundHandlerPostProcessor
import de.bringmeister.spring.aws.kinesis.KinesisOutboundStreamPostProcessor
import org.springframework.boot.autoconfigure.AutoConfigureBefore
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.validation.Validator

@Configuration
@ConditionalOnBean(Validator::class)
@ConditionalOnProperty("aws.kinesis.validate", matchIfMissing = true)
@AutoConfigureBefore(AwsKinesisAutoConfiguration::class)
class KinesisValidationAutoConfiguration(
    private val validator: Validator
) {

    @Bean
    fun validatingPostProcessor() = ValidatingPostProcessor(validator)
}
