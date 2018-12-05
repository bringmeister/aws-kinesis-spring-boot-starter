aws-kinesis-spring-boot-starter
===============================

[![Build Status](https://img.shields.io/travis/bringmeister/aws-kinesis-spring-boot-starter/master.svg)](https://travis-ci.org/bringmeister/aws-kinesis-spring-boot-starter)
[![Coverage Status](https://img.shields.io/coveralls/bringmeister/aws-kinesis-spring-boot-starter/master.svg)](https://coveralls.io/r/bringmeister/aws-kinesis-spring-boot-starter)
[![Release](https://img.shields.io/github/release/bringmeister/aws-kinesis-spring-boot-starter.svg)](https://github.com/bringmeister/aws-kinesis-spring-boot-starter/releases)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/bringmeister/aws-kinesis-spring-boot-starter/master/LICENSE)

## Dependencies

- Spring Boot 2.1.0 or higher
- [Jackson](https://github.com/FasterXML/jackson)

## Notes

This library is written in Kotlin. 
However, it's perfectly **compatible with Java**. 
You will find examples for both languages below.

## Installation

Add the following dependency to your project:
```
repositories {
    ...
    maven { url 'https://jitpack.io' }
}
compile "com.github.bringmeister:aws-kinesis-spring-boot-starter:+"
```

**Note:** See above for the latest version available!

## Configuration

In order to use this library you need to configure some properties in your `application.yml`. 
The following shows the minimal required configuration.
This configuration will allow you to send and receive messages.

```
aws:
  kinesis:
    region: eu-central-1
    consumer-group: example-service
    aws-account-id: "000000000000"
    iam-role-to-assume: ExampleKinesisRole
```

### Configuration Guide

#### Local development

Before your application goes live you typically want to develop and test your code locally.
To do so, we have used Docker.
You find a `docker-compose.yml` file in the root of this project.
Run `docker-compose up` in order to start Kinesis (and DynamoDB).

The configuration for local development looks like this:

```
aws:
  kinesis:
    region: local
    kinesis-url: http://localhost:14567
    consumer-group: example-service
    aws-account-id: "222222222222"
    iam-role-to-assume: ExampleKinesisRole
    createStreams: true
    dynamo-db-settings:
      url: http://localhost:14568
````
Any stream used in your application will be created (as soon as it is used first) if it does not exist.

Also, you must enable a Spring profile (`kinesis-local`):

```
spring:
  profiles:
    include: kinesis-local
```

You can also see `JavaListenerTest` and `KotlinListenerTest.kt` for running examples. 
Both tests will use the same Docker images in order to send and receive messages.

#### Creating streams automatically

You can create streams automatically by turning the `create-streams` flag on:

```
aws:
  kinesis:
    ...
    create-streams: true
```

By default, `create-streams` will be turned-off. 
So if you don't specify anything, no streams will be created.

#### Configuring initial position in stream

You can use one of following values:
* `LATEST`: Start after the most recent data record (fetch new data).
* `TRIM_HORIZON`: Start from the oldest available data record.

```
aws:
  kinesis:
    ...
    initial-position-in-stream: TRIM_HORIZON
```

If you don't specify anything, by default, `LATEST` value will be used.

#### Configuring listeners

You can configure listeners in order to use a dedicated role and account for a stream.

```
aws:
  kinesis:
    ...
    consumer:
      - stream-name: my-special-stream
        aws-account-id: "111111111111"
        iam-role-to-assume: SpecialKinesisConsumer
```

By default, events won't be retried. When the processing of an event fails (can't be deserialized for example), it will 
be skipped and the next event will be processed. Retrying of events can be activated in the configuration like this:

```
aws:
  kinesis:
    ...
    retry:
        maxRetries: 5
        backoffTimeInMilliSeconds: 1000 // wait 1 second between retries
    ...
```

#### Configure producers

You can configure producers in order to use a dedicated role and account for a stream.

```
aws:
  kinesis:
    ...
    producer:
      - stream-name: my-special-stream
        aws-account-id: "111111111111"
        iam-role-to-assume: SpecialKinesisConsumer
```

#### Disable automatic registration of `@KinesisListener`

Automatic registration of `@KinesisListener`-annotated methods can be disabled.

```yaml
aws:
  kinesis:
    listener:
        disabled: true
```

## Usage

### Publishing messages

Inject the `AwsKinesisOutboundGateway` wherever you like and pass stream name, data (the actual payload) and metadata to the `send()`-method.

Java example:

```Java
@Service
public class MyService {

    private final AwsKinesisOutboundGateway gateway;

    public MyService(AwsKinesisOutboundGateway gateway) {
        this.gateway = gateway;
    }

    public void sendMyMessage() {
        Record record = new Record(new MyMessage("my content"), new MyMetadata("my metadata"));
        gateway.send("my-stream", record); 
    }
}
```

See `JavaListenerTest.java` for an example.

Kotlin example:

```Kotlin
@Service
class MyService(private val gateway: AwsKinesisOutboundGateway) {
    fun sendMyMessage() {        
        val record = Record(MyMessage("my content"), MyMetadata("my metadata"))
        gateway.send("my-stream", record)
    }
}
```

See `KotlinListenerTest.kt` for an example.

The event will be marshalled as JSON using Jackson and send to the Kinesis stream using the credentials defined in the `application.yml`.

````
{
    "data":"my content",
    "metadata":"my metadata"
}
````

### Consuming messages

In order to consume messages, you need to annotate your listener method with the `KinesisListener` annotation.
Your class must be a Spring Bean annotated withg `@Service` or `@Component`.
It will be picked-up automatically and registered as a listener.
The listener method must take two arguments - one for the actual data and one for the meta data.

Java example:

```Java
@Service
public class MyKinesisListener {

    @KinesisListener(stream = "foo-stream")
    public void handle(MyData data, MyMetadata metadata) {
        System.out.println(data + ", " + metadata);
    }
}
```

See `JavaListenerTest.java` for an example.

Kotlin example:

```Kotlin
@Service
class MyKinesisListener {

    @KinesisListener(stream = "foo-stream")
    override fun handle(data: MyData, metadata: MyMetadata) = println("$data, $metadata")
}
```

See `KotlinListenerTest.kt` for an example.

## Developer Guide

We're using the official Kotlin Style Guide to format our code.
Follow the link below for more information and instructions on how to configure the IntelliJ formatter according to this style guide.

More:

* https://kotlinlang.org/docs/reference/coding-conventions.html
