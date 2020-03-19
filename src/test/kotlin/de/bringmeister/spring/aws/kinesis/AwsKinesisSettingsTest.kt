package de.bringmeister.spring.aws.kinesis

import de.bringmeister.spring.aws.kinesis.ConfigurationPropertiesBuilder.Companion.builder
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration
import org.springframework.boot.context.properties.bind.BindException
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean
import software.amazon.kinesis.common.InitialPositionInStream
import java.time.Duration

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [ValidationAutoConfiguration::class])
class AwsKinesisProducerSettingsTest {

    @Autowired
    private lateinit var localValidatorFactoryBean: LocalValidatorFactoryBean

    @Test
    fun `should read streams settings`() {

        val settings = builder<AwsKinesisSettings>()
            .withPrefix("aws.kinesis")
            .withProperty("region", "local")
            .withProperty("kinesis-url", "http://localhost:14567")
            .withProperty("streams[0].streamName", "foo-event-stream")
            .withProperty("streams[0].awsAccountId", "222222222222")
            .withProperty("streams[0].iamRoleToAssume", "ExampleKinesisRole")
            .validateUsing(localValidatorFactoryBean)
            .build()

        assertThat(settings.streams[0].streamName).isEqualTo("foo-event-stream")
        assertThat(settings.streams[0].awsAccountId).isEqualTo("222222222222")
        assertThat(settings.streams[0].iamRoleToAssume).isEqualTo("ExampleKinesisRole")
    }

    @Test
    fun `should read default settings`() {

        val settings = builder<AwsKinesisSettings>()
            .withPrefix("aws.kinesis")
            .withProperty("region", "eu-central-1")
            .validateUsing(localValidatorFactoryBean)
            .build()

        assertThat(settings.region).isEqualTo("eu-central-1")
        assertThat(settings.kinesisUrl).isEqualTo("https://kinesis.eu-central-1.amazonaws.com")
        assertThat(settings.dynamoDbSettings!!.url).isEqualTo("https://dynamodb.eu-central-1.amazonaws.com")
        assertThat(settings.initialPositionInStream).isEqualTo(InitialPositionInStream.LATEST)
    }

    @Test
    fun `should override default settings`() {

        val kinesisUrl = "http://localhost:1234/kinesis"
        val dynamoDbUrl = "http://localhost:1234/dynamodb"
        val settings = builder<AwsKinesisSettings>()
            .withPrefix("aws.kinesis")
            .withProperty("region", "local")
            .withProperty("kinesisUrl", kinesisUrl)
            .withProperty("dynamoDbSettings.url", dynamoDbUrl)
            .withProperty("initialPositionInStream", "TRIM_HORIZON")
            .validateUsing(localValidatorFactoryBean)
            .build()

        assertThat(settings.region).isEqualTo("local")
        assertThat(settings.kinesisUrl).isEqualTo(kinesisUrl)
        assertThat(settings.dynamoDbSettings!!.url).isEqualTo(dynamoDbUrl)
        assertThat(settings.initialPositionInStream).isEqualTo(InitialPositionInStream.TRIM_HORIZON)
    }

    @Test(expected = BindException::class)
    fun `should fail if region is missing`() {

        builder<AwsKinesisSettings>()
            .withPrefix("aws.kinesis")
            .withProperty("kinesis-url", "http://localhost:14567")
            .validateUsing(localValidatorFactoryBean)
            .build()
    }

    @Test
    fun `should allow checkpointing retry configuration`() {

        val settings = builder<AwsKinesisSettings>()
            .withPrefix("aws.kinesis")
            .withProperty("region", "eu-central-1")
            .withProperty("checkpointing.retry.maxRetries", "3")
            .withProperty("checkpointing.retry.backoff", "23s")
            .validateUsing(localValidatorFactoryBean)
            .build()

        assertThat(settings.checkpointing.retry.maxRetries).isEqualTo(3)
        assertThat(settings.checkpointing.retry.backoff).isEqualTo(Duration.ofSeconds(23))
    }

    @Test(expected = BindException::class)
    fun `should fail if setting initialPositionInStream is not an enum value`() {
        val kinesisUrl = "http://localhost:1234/kinesis"
        val dynamoDbUrl = "http://localhost:1234/dynamodb"
        builder<AwsKinesisSettings>()
            .withPrefix("aws.kinesis")
            .withProperty("region", "local")
            .withProperty("kinesisUrl", kinesisUrl)
            .withProperty("dynamoDbSettings.url", dynamoDbUrl)
            .withProperty("initialPositionInStream", "WRONG_VALUE")
            .validateUsing(localValidatorFactoryBean)
            .build()
    }

    @Test
    fun `should infer kinesisUrl from region`() {
        val settings = builder<AwsKinesisSettings>()
            .withPrefix("aws.kinesis")
            .withProperty("region", "some-region")
            .build()
        assertThat(settings.kinesisUrl)
            .isNotNull()
            .isEqualTo("https://kinesis.some-region.amazonaws.com")
    }

    @Test
    fun `should infer dynamodbUrl from region`() {
        val settings = builder<AwsKinesisSettings>()
            .withPrefix("aws.kinesis")
            .withProperty("region", "some-region")
            .build()
        assertThat(settings.dynamoDbSettings).isNotNull
        assertThat(settings.dynamoDbSettings!!.url)
            .isNotNull()
            .isEqualTo("https://dynamodb.some-region.amazonaws.com")
    }

    @Test
    fun `should return null urls when urls and regions are not set`() {
        // This test is for our flaky coveralls mainly, because it sometimes
        // reports the <return null> lines and sometimes doesn't.
        val settings = builder<AwsKinesisSettings>()
            .withPrefix("aws.kinesis")
            .withProperty("initialPositionInStream", "TRIM_HORIZON")
            .build()
        assertThat(settings.kinesisUrl).isNull()
        assertThat(settings.dynamoDbSettings).isNull()
    }
}
