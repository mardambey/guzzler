Guzzler: Stream MySQL binary logs and act on them
=================================================

Guzzler allows you to stream MySQL binary logs from a master and lets you
act on them using Scala actors (consumers). Consumers are configurable in
guzzler.conf along with the rest of the required parameters. Included with
Guzzler is a dummy consumer and a RabbitMQ one that will push queries into a
RabbitMQ server for consumption.

Consumers either in Guzzler itself of behind RabbitMQ can analyze the
queries (Guzzler provides an SQL query parser based on ZQL) and may decide
to update counters, fire off events, log messages, etc.

ZQL: http://www.gibello.com/code/zql/
=====================================
