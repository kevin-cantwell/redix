# Redix

Redix acts as a Redis server proxy and expands the default set of Redis commands with a few of its own.

## Special Commands

The proxy extends the set of Redis commands with the following special commands:

* [PROMOTE](#promote-host-port-auth)

#### PROMOTE host port [auth]

_This is a work in progress! Use at your own peril!_

The PROMOTE command executes a seamless failover to a slave Redis instance. The proxy will begin to buffer all in-flight requests and wait for the slave replication offset to fully sync. Once the slave is synced, a `SLAVEOF NO ONE` command is issued and it becomes the master. All buffered requests are then flushed to the master instance and a `SLAVEOF host port` command is sent to the previous master (now demoted), thereby completing the promotion.
