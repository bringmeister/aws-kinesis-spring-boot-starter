package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.nhaarman.mockito_kotlin.mock
import de.bringmeister.spring.aws.kinesis.AwsKinesisStreamInitializingReadyEventTest.WorkerInitializedListener
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Service
import org.springframework.test.context.junit4.SpringRunner
import java.util.concurrent.CountDownLatch
import javax.validation.Validator

@SpringBootTest(classes = [WorkerInitializedListener::class])
@RunWith(SpringRunner::class)
class AwsKinesisStreamInitializingReadyEventTest {

    val recordMapper = mock<ReflectionBasedRecordMapper>()
    val configuration = mock<RecordProcessorConfiguration>()
    val validator = mock<Validator>()
    val handlerMock = mock<(FooCreatedEvent, EventMetadata) -> Unit> { }
    final var handler = object {
        @KinesisListener(stream = "foo-event-stream")
        fun handle(data: FooCreatedEvent, metadata: EventMetadata) {
            handlerMock.invoke(data, metadata)
        }
    }

    val initializationInput = mock<InitializationInput> {
        on { shardId }.thenReturn("any-shard")
    }

    val kinesisListener = KinesisListenerProxyFactory(AopProxyUtils()).proxiesFor(handler)[0]

    @Autowired
    lateinit var publisher: ApplicationEventPublisher

    @Autowired
    lateinit var workerInitializedListener: WorkerInitializedListener

    @Test
    fun `should publish initializing ready event`() {
        AwsKinesisRecordProcessor(recordMapper, configuration, kinesisListener, publisher, validator)
            .initialize(initializationInput)
        countDownLatch.await()

        assertEquals(events[0], WorkerInitializedEvent("foo-event-stream", "any-shard"))
    }

    @Service
    class WorkerInitializedListener {
        @EventListener
        fun workerInitialized(event: WorkerInitializedEvent) {
            events.add(event)
            countDownLatch.countDown()
        }
    }

    companion object {
        var events = mutableListOf<WorkerInitializedEvent>()
        val countDownLatch = CountDownLatch(1)
    }
}
