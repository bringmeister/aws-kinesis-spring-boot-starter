package de.bringmeister.spring.aws.kinesis

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import org.junit.Test

class AwsKinesisInboundGatewayTest {

    val workerStarter: WorkerStarter = mock { }
    val worker = mock<Worker> { }
    val kinesisListenerProxy = KinesisListenerProxy(mock { }, mock { }, "my-stream")
    val workerFactory: WorkerFactory = mock {
        on {
            worker(kinesisListenerProxy)
        } doReturn worker
    }

    val inboundGateway = AwsKinesisInboundGateway(workerFactory, workerStarter)

    @Test
    fun `when registering a listener instance it should create worker`() {
        inboundGateway.register(kinesisListenerProxy)
        verify(workerFactory).worker(kinesisListenerProxy)
    }

    @Test
    fun `when registering a listener instance it should run worker`() {
        inboundGateway.register(kinesisListenerProxy)
        verify(workerStarter).start(worker)
    }
}
