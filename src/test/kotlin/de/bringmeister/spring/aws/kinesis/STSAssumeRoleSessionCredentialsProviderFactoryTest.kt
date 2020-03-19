package de.bringmeister.spring.aws.kinesis

import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import com.nhaarman.mockito_kotlin.whenever
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider

class STSAssumeRoleSessionCredentialsProviderFactoryTest {

    val credentialsProvider = mock<AwsCredentialsProvider> {}
    val settings = mock<AwsKinesisSettings> {
        on { this.region } doReturn "eu-central-1"
        on { this.kinesisUrl } doReturn "http://example.org"
    }

    @Test
    fun `should create sts credentials provider `() {
        val unit = STSAssumeRoleSessionCredentialsProviderFactory(credentialsProvider, settings)

        val credentialsProvider = unit.credentials("anyRole")

        assertThat(credentialsProvider).isInstanceOf(StsAssumeRoleCredentialsProvider::class.java)
    }

    @Test
    fun `should use configured region`() {
        val unit = STSAssumeRoleSessionCredentialsProviderFactory(credentialsProvider, settings)

        unit.credentials("anyRole")

        verify(settings).region
    }

    @Test
    fun `should use credentials when set`() {

        val credentials = RoleCredentials().apply {
            accessKey = "access"
            secretKey = "secret"
        }
        whenever(settings.getRoleCredentials("roleWithCredentials")).thenReturn(credentials)

        val unit = STSAssumeRoleSessionCredentialsProviderFactory(credentialsProvider, settings)

        unit.credentials("roleWithCredentials")

        // it's not really possible to test the credentials, since they are thrown against a token service
        verify(settings).getRoleCredentials("roleWithCredentials")
    }
}
