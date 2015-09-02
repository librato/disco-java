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

```java
CuratorFramework framework; // Initialize this
SelectorStrategy selector = new RoundRobinSelectorStrategy();
DiscoClient client = new DiscoClient(framework, serviceName, selector);
client.start(host, port);

Optional<Node> node = client.getServiceNode();
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
DiscoService service = new DiscoService(framework, "myservice", true);
service.start("hostname", 4321);
```

As long as the service is running, this configuration will be associated with the
Zookeeper node `/services/myservice/nodes/hostname:4321`. Upon stopping the
service, the node will be removed from Zookeeper. The third parameter dictates
whether the service adds a shutdown hook to stop the disco service. This is
useful because of you want to remove the service from discovery _before_
peforming a full shutdown, for example before shutting down the HTTP port.

```java
service.stop();
```

## Testing

Run tests with `mvn test`. **Note**: tests assume you have Zookeeper running on
`localhost:2181`
