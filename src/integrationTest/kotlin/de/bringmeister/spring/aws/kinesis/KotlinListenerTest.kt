package de.bringmeister.spring.aws.kinesis

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.junit4.SpringRunner
import org.testcontainers.containers.GenericContainer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import de.bringmeister.spring.aws.kinesis.creation.KinesisCreateStreamAutoConfiguration
import de.bringmeister.spring.aws.kinesis.validation.KinesisValidationAutoConfiguration

@ActiveProfiles("kinesis-local")
@SpringBootTest(
    classes = [
        KotlinTestListener::class,
        JacksonConfiguration::class,
        JacksonAutoConfiguration::class,
        KinesisLocalConfiguration::class,
        AwsKinesisAutoConfiguration::class,
        KinesisCreateStreamAutoConfiguration::class,
        KinesisValidationAutoConfiguration::class
    ]
)
@RunWith(SpringRunner::class)
class KotlinListenerTest {

    @Autowired
    lateinit var outbound: AwsKinesisOutboundGateway

    companion object {

        val latch = CountDownLatch(1)

        class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)

        @ClassRule
        @JvmField
        val kinesis = KGenericContainer("instructure/kinesalite:latest").withCreateContainerCmdModifier({
            it.withPortBindings(Ports(PortBinding(Ports.Binding("localhost", "14567"), ExposedPort.tcp(4567))))
        })

        @ClassRule
        @JvmField
        val dynamodb = KGenericContainer("richnorth/dynalite:latest").withCreateContainerCmdModifier({
            it.withPortBindings(Ports(PortBinding(Ports.Binding("localhost", "14568"), ExposedPort.tcp(4567))))
        })
    }

    @Test
    fun `should send and receive events`() {

        val fooEvent = FooCreatedEvent("any-field")
        val metadata = EventMetadata("test")

        outbound.send("foo-event-stream", Record(fooEvent, metadata))

        latch.await(1, TimeUnit.MINUTES) // wait for event-listener thread to process event

        // If we come to this point, the LATCH was counted down!
        // This means the event has been consumed - test succeeded!
    }
}

