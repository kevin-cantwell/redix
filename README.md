# HARedis

This is a tool for migrating to a new redis server with zero downtime. It acts as a Redis server proxy and adds the following commands:


## Special Commands

The proxy extends the set of Redis commands with the following special commands:

* [PROMOTE](#PROMOTE-host-port)

#### PROMOTE host port

The PROMOTE command executes a seamless failover to a slave Redis instance. The proxy will begin to buffer all in-flight requests and wait for the slave replication offset to fully sync. Once the slave is synced, a `SLAVEOF NO ONE` command is issued and it becomes the master. All buffered requests are then flushed to the master instance and a `SLAVEOF host port` command is sent to the previous master (now demoted), thereby completing the promotion.
