package de.bringmeister.spring.aws.kinesis;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;
import de.bringmeister.spring.aws.kinesis.creation.KinesisCreateStreamAutoConfiguration;
import de.bringmeister.spring.aws.kinesis.metrics.KinesisMetricsAutoConfiguration;
import de.bringmeister.spring.aws.kinesis.validation.KinesisValidationAutoConfiguration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@ActiveProfiles("kinesis-local")
@SpringBootTest(
    classes = {
        JavaTestListener.class,
        JacksonConfiguration.class,
        JacksonAutoConfiguration.class,
        KinesisLocalConfiguration.class,
        AwsKinesisAutoConfiguration.class,
        KinesisCreateStreamAutoConfiguration.class,
        KinesisValidationAutoConfiguration.class,
        KinesisMetricsAutoConfiguration.class
    },
    properties = {
        "aws.kinesis.initial-position-in-stream: TRIM_HORIZON"
    }
)
@RunWith(SpringRunner.class)
public class JavaListenerTest {

    @Autowired
    private AwsKinesisOutboundGateway outbound;

    public static CountDownLatch LATCH = new CountDownLatch(1);

    @ClassRule
    public static GenericContainer KINESIS_CONTAINER = new GenericContainer("instructure/kinesalite:latest").withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
        @Override
        public void accept(CreateContainerCmd createContainerCmd) {
            createContainerCmd.withPortBindings(new Ports(new PortBinding(new Ports.Binding("localhost", "14567"), ExposedPort.tcp(4567))));
        }
    });

    @ClassRule
    public static GenericContainer DYNAMODB_CONTAINER = new GenericContainer("richnorth/dynalite:latest").withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
        @Override
        public void accept(CreateContainerCmd createContainerCmd) {
            createContainerCmd.withPortBindings(new Ports(new PortBinding(new Ports.Binding("localhost", "14568"), ExposedPort.tcp(4567))));
        }
    });

    @Test
    public void should_send_and_receive_events() throws InterruptedException {

        FooCreatedEvent fooEvent = new FooCreatedEvent("any-field");
        EventMetadata metadata = new EventMetadata("test");

        outbound.send("foo-event-stream", new Record(fooEvent, metadata));

        boolean messageReceived = LATCH.await(1, TimeUnit.MINUTES); // wait for event-listener thread to process event

        assertThat(messageReceived).isTrue();
    }
}
