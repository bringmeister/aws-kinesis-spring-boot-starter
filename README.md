# aws-kinesis-spring-boot-starter
[![Build Status](https://img.shields.io/travis/bringmeister/aws-kinesis-spring-boot-starter/master.svg)](https://travis-ci.org/bringmeister/aws-kinesis-spring-boot-starter)
[![Coverage Status](https://img.shields.io/coveralls/bringmeister/aws-kinesis-spring-boot-starter/master.svg)](https://coveralls.io/r/bringmeister/aws-kinesis-spring-boot-starter)
[![Release](https://img.shields.io/github/release/bringmeister/aws-kinesis-spring-boot-starter.svg)](https://github.com/bringmeister/aws-kinesis-spring-boot-starter/releases)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/bringmeister/aws-kinesis-spring-boot-starter/master/LICENSE)

## Dependencies
- Spring 4.3.0 or higher
- [Jackson](https://github.com/FasterXML/jackson)

## Installation
Add the following dependency to your project:
```
repositories {
    ...
    maven { url 'https://jitpack.io' }
}
compile 'com.github.bringmeister:aws-kinesis-spring-boot-starter:v0.0.2'
```

## Configuration
### 1. Define event streams in application.yml

Configure the streams you want to consume and/or publish to in your application.yml
by providing the stream name, the id of the aws account this streams belongs to and 
iam role that allows you to read and/or write to the stream.
```
aws:
  kinesis:
    consumerGroup: example-service
    kinesisUrl: "https://kinesis.eu-central-1.amazonaws.com" #optional
    region: eu-cental-1 #optional
    consumer:
      - streamName: foo-event-stream
        awsAccountId: "000000000000"
        iamRoleToAssume: ExampleServiceKinesisConsumer
        metricsLevel: DETAILED #optional
        dynamoDBSettings:
            url: "https://dynamodb.eu-central-1.amazonaws.com" #optional
            leaseTableReadCapacity: 5 #optional
            leaseTableWriteCapacity: 8 #optional
      - streamName: bar-event-stream
        awsAccountId: "111111111111"
        iamRoleToAssume: ExampleServiceKinesisConsumer
    producer:
      - streamName: foo-event-stream
        awsAccountId: "000000000000"
        iamRoleToAssume: ExampleServiceKinesisProducer
```

### 2. Define kinesis event
Define the kinesis event class by implementing `de.bringmeister.spring.aws.kinesis.KinesisEvent` interface.

```kotlin
data class FooCreatedKinesisEvent(val data: FooCreatedEvent, val metadata: MyEventMetadata) : KinesisEvent {
    companion object {
        const val STREAM_NAME = "foo-event-stream" //maps to stream name in application.yml
    }
    override fun streamName() = STREAM_NAME
    override fun data() = data
    override fun metadata() = metadata
    
}
data class FooCreatedEvent(val foo: String)
data class MyEventMetadata(val occurredAt: OffsetDateTime)
```

## Usage
aws-kinesis-spring-boot-starter provides two spring-beans for publishing and consuming of event streams:
- `de.bringmeister.spring.aws.kinesis.AwsKinesisInboundGateway`
- `de.bringmeister.spring.aws.kinesis.AwsKinesisOutboundGateway`

### 2. Publish Events
Inject `AwsKinesisOutboundGateway`-bean wherever you like and pass an event to the `send()`-method.
```kotlin
fun <PayloadType, MetadataType> send(event: KinesisEvent<PayloadType, MetadataType>)
```

Example:
```kotlin
@Service
class EventProducer(private val gateway: AwsKinesisOutboundGateway) {
    fun sendAnyFooEvent() {        
        gateway.send(FooCreatedKinesisEvent(data = FooCreatedEvent(foo = "anything"), metadata = EventMetadata(occurredAt = OffsetDateTime.now())))
    }
}
```

### 3. Consume Events
Inject `AwsKinesisInboundGateway` wherever you like and call `listen()`. You need to provide the name of the stream you defined in the application.yml, 
the class of the kinsis event and the method to process the payload data load wrapped by the kinesis event.
```kotlin
fun <PayloadType, KinesisEventType : KinesisEvent<PayloadType, *>, KinesisEventClassType : Class<KinesisEventType>>
            listen(streamName: String, eventClass: KinesisEventClassType, payloadHandler: (PayloadType) -> Unit)
```

Example:
```kotlin
@Service
class EventConsumer(private val gateway: AwsKinesisInboundGateway) {
    
    @Scheduled(fixedDelay = (60 * 1000))
    fun listenForFooEvents() {
        gateway.listen(FooEvent.STREAM_NAME, FooCreatedKinesisEvent::class.java, { event:FooCreatedEvent -> println("Processing Event $event") })
    }
}
```