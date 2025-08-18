# lnresearch Processing Pipeline

This is a processing pipeline that takes messages from a rabbitmq
queue, processes them, and then puts them back onto the queue. This
allows us to do step-wise refinement of the data, and ultimately
archival of historical information. The app is essentially a web 
app, showing the current status, with a number of status indicators for the 
various background services.

## Flow

We have the `lnr.gossip.raw` exchange on the configured rabbitmq
server to which we submit all raw messages, independently of source,
and sometimes event wrapped in protobuf envelopes.

One of the ingest tasks is the `glbridge`, which subscribes to a rabbitmq queue on a different upstream server, and replicates the messages into the local `lnr.gossip.raw` exchange.

The `dedup` workflow will load messages from the `lnr.gossip.raw`
exchange, then checks that the message starts with one of the
permitted types (see below), and potentially stripping the protobuf
envelope. Once we have the raw message, we check in a sqlite3 database
whether we already know the message or not, and if we don't know it we
send it back to the `lnr.gossip.uniq` exchange. The sqlite3 database
is very simple with just a single table `messages` which has a single
column `raw` that is `UNIQUE`. Inserting into this table, and checking
if we failed will tell us whether it is a new message, and therefore
should be forwarded, or if it is a duplicate, and should be dropped.

Finally, we have an `archive` workflow, that listens for messages in
`lnr.gossip.uniq` exchange by bindings its own `lnr.gossip.archiver`
queue to the `lnr.gossip.uniq` exchange. The archiver takes hourly or
daily snapshots, streaming messages as they come in to a file in
`annex/dailies`, rotating files whenever the day or hour ends. A
config value as envvar is used to determine whether to do hourly or
daily snapshots.

## Information type

We manage Lightning Network Gossip messages, i.e., binary messages
that start with `\0x01\x00`, `\0x01\0x01` and `\0x01\0x02`.

## Best practices

 - Use `uv` to manage the virtual environment, pinning python 3.10
 - Use `fastapi` and `sqlmodel` with `async` handlers
 - Use aio_pika for rabbitmq interactions.
 - Create a `Config` class to collect all environment variable-based config values
 - The root package is `lnr`
 - Use `jj` for version control. After each feature, use `jj describe` to add a description, and then use `jj new` to start a new commit for the next feature.
 - Use `rich` and `logging` to add nice logs to the app
