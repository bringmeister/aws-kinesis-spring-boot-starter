package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import org.junit.Test

class AwsKinesisInboundGatewayTest {

    val workerStarter: WorkerStarter = mock { }
    val worker = mock<Worker> { }
    val recordDeserializer = mock<RecordDeserializer> { }
    val kinesisListenerProxy = KinesisListenerProxy(mock { }, mock { }, "my-stream")
    val workerFactory: WorkerFactory = mock {
        on {
            worker(kinesisListenerProxy, recordDeserializer)
        } doReturn worker
    }

    val inboundGateway = AwsKinesisInboundGateway(workerFactory, workerStarter)

    @Test
    fun `when registering a listener instance it should create worker`() {
        inboundGateway.register(kinesisListenerProxy, recordDeserializer)
        verify(workerFactory).worker(kinesisListenerProxy, recordDeserializer)
    }

    @Test
    fun `when registering a listener instance it should run worker`() {
        inboundGateway.register(kinesisListenerProxy, recordDeserializer)
        verify(workerStarter).start(worker)
    }
}
