package de.bringmeister.spring.aws.kinesis

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import de.bringmeister.spring.aws.kinesis.AwsKinesisSettingsTestFactory.Companion.settings
import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Test

class KinesisClientProviderTest {

    private val credentialsProviderFactory: AwsCredentialsProviderFactory = mock { }
    private val settings = settings().withRequired().withDefaults().withProducerFor("my-stream").build()
    private val clientProvider = KinesisClientProvider(credentialsProviderFactory, settings)

    @Before
    fun setUp() {
        whenever(credentialsProviderFactory.credentials(any())).thenReturn(mock { })
    }

    @Test
    fun `should create Kinesis client with detailed settings`() {
        val client = clientProvider.clientFor("my-stream")
        assertThat(client).isNotNull
        verify(credentialsProviderFactory).credentials("arn:aws:iam::100000000042:role/kinesis-user-role")
    }

    @Test
    fun `should create Kinesis client with default settings`() {
        val client = clientProvider.clientFor("unknown-stream")
        assertThat(client).isNotNull
        verify(credentialsProviderFactory).credentials("arn:aws:iam::100000000042:role/kinesis-user-role")
    }
}
