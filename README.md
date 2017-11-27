# Fault-tolerant RabbitMQ Topic Partitioning

## Failover Protocol

- run two producers nodes and one consumer
- kill consumer and immediately kill master node

```
 [<-] c1 received: 6 via g
 [<-] c1 received: 7 via h
 [<-] c1 received: 7 via h
^C [**] c1 trying to shutdown..
shutting down heartbeats
shutting down consumer
 [**] c1 graceful shutdown
```

Master has no chance to rebalance

```
 [->] Sending 8 with route a
SIGKILL
```

Slave producer takes over and runs cleanup procedure, recovers three stuck messages.

```
 [->] Sending 8 with route a
 [->] Sending 9 with route b
 -> removing consumer from pool: c1
 -> assuming partition master
 -> master assumed, rebalance and checking recently exited consumers
 -> balancing for 0 consumers
slave exiting check-outs
slave exiting balancer
slave exiting check-ins
 XX retiring stuck queue c1
 -> cleanup unbinding c1 from a
 -> command manager running
 -> cleanup unbinding c1 from b
 -> cleanup unbinding c1 from c
 -> cleanup unbinding c1 from d
 -> cleanup unbinding c1 from e
 -> cleanup unbinding c1 from f
 -> cleanup unbinding c1 from g
 -> cleanup unbinding c1 from h
 -> cleanup draining 3 messages
 => cleanup drain 8 from c1
 => cleanup drain 8 from c1
 => cleanup drain 9 from c1
 -> cleanup c1 drained.
 -> cleanup deleting queue c1
 XX rebalancing after config change
 -> balancing for 0 consumers
detaching slave consumer
```

Add new consumer c2, the new master resends the messages

```
 -> adding consumer to pool: c2
command received balance:c2:1
 -> balancing for 1 consumers
binding c2 to a
binding c2 to b
binding c2 to c
binding c2 to d
binding c2 to e
binding c2 to f
binding c2 to g
binding c2 to h
 [->] Sending 10 with route c
 [=>] Sending 8 with route a (drain)
 [=>] Sending 8 with route a (drain)
 [=>] Sending 9 with route b (drain)
 [->] Sending 11 with route d
 [->] Sending 12 with route e
```

The consumer c2 receives the stuck messages after rebinding

```
 [**] c2 running
 [<-] c2 received: 10 via c
 [<-] c2 received: 8 via a
 [<-] c2 received: 8 via a
 [<-] c2 received: 9 via b
 [<-] c2 received: 11 via d
 [<-] c2 received: 12 via e
```

No messages got lost.

