[![Build Status](https://travis-ci.org/librato/disco-java.svg?branch=master)](https://travis-ci.org/librato/disco-java)

# Disco

Simple zookeeper-based service discovery.

## Requirements

 * Java 1.7
 * Zookeeper 3.4.5+

This library uses Zookeeper nodes to maintain a set of available hosts for
a configured service. A client is automatically updated by Zookeeper/Curator
when a service is added or removed.

## Client usage

The server allows you to save arbitrary data per node that is accessible from
the client via the `Decoder<T>` interface.

```java
CuratorFramework framework; // Initialize this
SelectorStrategy selector = new RoundRobinSelectorStrategy();
Decoder<T> decoder; // Initialize this
DiscoClient<> client = new DiscoClient<T>(framework, serviceName, selector, decoder);
client.start(host, port);

Optional<Node<T>> node = client.getServiceNode();
```

Based on the selector strategy, the service will return the nodename of a
connected service, or Optional.absent() if none are connected.

Stop the client on shutdown to cleanly disconnect from Zookeeper.

```java
client.stop();
```

## Service usage

```java
CuratorFramework framework; // Initialize this
byte[] payload; // Initialize this
DiscoService service = new DiscoService(framework, "myservice");
service.start("hostname", 4321, true, payload);
```

As long as the service is running, this configuration will be associated with the
Zookeeper node `/services/myservice/nodes/hostname:4321` and the `byte[] 
payload` as the node's data. Upon stopping the service, the node will be
removed from Zookeeper. The third parameter dictates whether the service adds a
shutdown hook to stop the disco service. This is useful because of you want to
remove the service from discovery _before_ peforming a full shutdown, for
example before shutting down the HTTP port.

```java
service.stop();
```

## Testing

Run tests with `mvn test`. **Note**: tests assume you have Zookeeper running on
`localhost:2181`
