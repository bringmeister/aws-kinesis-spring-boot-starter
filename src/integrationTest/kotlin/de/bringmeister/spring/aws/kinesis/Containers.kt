package de.bringmeister.spring.aws.kinesis

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import org.testcontainers.containers.GenericContainer

object Containers {
    class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)

    fun kinesis() = KGenericContainer("instructure/kinesalite:latest").withCreateContainerCmdModifier {
        it.withPortBindings(Ports(PortBinding(Ports.Binding("localhost", "14567"), ExposedPort.tcp(4567))))
    }

    fun dynamoDb() = KGenericContainer("richnorth/dynalite:latest").withCreateContainerCmdModifier {
        it.withPortBindings(Ports(PortBinding(Ports.Binding("localhost", "14568"), ExposedPort.tcp(4567))))
    }
}