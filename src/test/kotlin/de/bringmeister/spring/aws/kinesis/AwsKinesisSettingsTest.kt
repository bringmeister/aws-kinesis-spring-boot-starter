package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import de.bringmeister.spring.aws.kinesis.ConfigurationPropertiesBuilder.Companion.builder
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.validation.BindException
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [ValidationAutoConfiguration::class])
class AwsKinesisProducerSettingsTest {

    @Autowired
    private lateinit var localValidatorFactoryBean: LocalValidatorFactoryBean

    @Test
    fun `should read producer settings`() {

        val settings = builder<AwsKinesisSettings>()
            .populate(AwsKinesisSettings())
            .withPrefix("aws.kinesis")
            .withProperty("region", "local")
            .withProperty("kinesis-url", "http://localhost:14567")
            .withProperty("producer[0].streamName", "foo-event-stream")
            .withProperty("producer[0].awsAccountId", "222222222222")
            .withProperty("producer[0].iamRoleToAssume", "ExampleKinesisProducerRole")
            .validateUsing(localValidatorFactoryBean)
            .build()

        assertThat(settings.producer[0].streamName, equalTo("foo-event-stream"))
        assertThat(settings.producer[0].awsAccountId, equalTo("222222222222"))
        assertThat(settings.producer[0].iamRoleToAssume, equalTo("ExampleKinesisProducerRole"))
    }

    @Test
    fun `should read consumer settings`() {

        val settings = builder<AwsKinesisSettings>()
            .populate(AwsKinesisSettings())
            .withPrefix("aws.kinesis")
            .withProperty("region", "local")
            .withProperty("kinesis-url", "http://localhost:14567")
            .withProperty("consumer[0].streamName", "foo-event-stream")
            .withProperty("consumer[0].awsAccountId", "111111111111")
            .withProperty("consumer[0].iamRoleToAssume", "ExampleKinesisConsumerRole")
            .validateUsing(localValidatorFactoryBean)
            .build()

        assertThat(settings.consumer[0].streamName, equalTo("foo-event-stream"))
        assertThat(settings.consumer[0].awsAccountId, equalTo("111111111111"))
        assertThat(settings.consumer[0].iamRoleToAssume, equalTo("ExampleKinesisConsumerRole"))
    }

    @Test
    fun `should read default settings`() {

        val settings = builder<AwsKinesisSettings>()
            .populate(AwsKinesisSettings())
            .withPrefix("aws.kinesis")
            .withProperty("region", "eu-central-1")
            .validateUsing(localValidatorFactoryBean)
            .build()

        assertThat(settings.region, equalTo("eu-central-1"))
        assertThat(settings.kinesisUrl, equalTo("https://kinesis.eu-central-1.amazonaws.com"))
        assertThat(settings.dynamoDbSettings!!.url, equalTo("https://dynamodb.eu-central-1.amazonaws.com"))
        assertThat(settings.initialPositionInStream, equalTo(InitialPositionInStream.LATEST))
    }

    @Test
    fun `should override default settings`() {

        val kinesisUrl = "http://localhost:1234/kinesis"
        val dynamoDbUrl = "http://localhost:1234/dynamodb"
        val settings = builder<AwsKinesisSettings>()
            .populate(AwsKinesisSettings())
            .withPrefix("aws.kinesis")
            .withProperty("region", "local")
            .withProperty("kinesisUrl", kinesisUrl)
            .withProperty("dynamoDbSettings.url", dynamoDbUrl)
            .withProperty("initialPositionInStream", "TRIM_HORIZON")
            .validateUsing(localValidatorFactoryBean)
            .build()

        assertThat(settings.region, equalTo("local"))
        assertThat(settings.kinesisUrl, equalTo(kinesisUrl))
        assertThat(settings.dynamoDbSettings!!.url, equalTo(dynamoDbUrl))
        assertThat(settings.initialPositionInStream, equalTo(InitialPositionInStream.TRIM_HORIZON))
    }

    @Test(expected = BindException::class)
    fun `should fail if region is missing`() {

        builder<AwsKinesisSettings>()
            .populate(AwsKinesisSettings())
            .withPrefix("aws.kinesis")
            .withProperty("kinesis-url", "http://localhost:14567")
            .validateUsing(localValidatorFactoryBean)
            .build()
    }

    @Test
    fun `should allow retry configuration`() {

        val settings = builder<AwsKinesisSettings>()
            .populate(AwsKinesisSettings())
            .withPrefix("aws.kinesis")
            .withProperty("region", "eu-central-1")
            .withProperty("retry.maxRetries", "3")
            .withProperty("retry.backoffTimeInMilliSeconds", "23")
            .validateUsing(localValidatorFactoryBean)
            .build()

        assertThat(settings.retry.maxRetries, equalTo(3))
        assertThat(settings.retry.backoffTimeInMilliSeconds, equalTo(23L))
    }

    @Test(expected = BindException::class)
    fun `should fail if setting initialPositionInStream is not an enum value`() {
        val kinesisUrl = "http://localhost:1234/kinesis"
        val dynamoDbUrl = "http://localhost:1234/dynamodb"
        builder<AwsKinesisSettings>()
            .populate(AwsKinesisSettings())
            .withPrefix("aws.kinesis")
            .withProperty("region", "local")
            .withProperty("kinesisUrl", kinesisUrl)
            .withProperty("dynamoDbSettings.url", dynamoDbUrl)
            .withProperty("initialPositionInStream", "WRONG_VALUE")
            .validateUsing(localValidatorFactoryBean)
            .build()
    }
}
