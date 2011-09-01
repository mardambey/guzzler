Guzzler: Stream MySQL binary logs and act on them
=================================================

Guzzler allows you to stream MySQL binary logs from a master and lets you
act on them using Scala actors (consumers). Consumers are configurable in
guzzler.conf along with the rest of the required parameters. Included with
Guzzler is a dummy consumer and a RabbitMQ one that will push queries into a
RabbitMQ server for consumption.

Guzzler can start, stop, restart and seek in the binary logs via a remote
SSH interface. Guzzler can also buffer binary logs locally while external
consumers are restarted or upgraded.

Consumers either in Guzzler itself of behind RabbitMQ can analyze the
queries (Guzzler provides an SQL query parser based on ZQL) and may decide
to update counters, fire off events, log messages, etc.

Sample SSH session (no auth yet):

    -=[ Welcome to Guzzler ]=-
    
    guzzler stream stop
    guzzler stream start
    guzzler stream seek db-bin.000924 900993280
    guzzler queue pause
    guzzler queue resume

ZQL: http://www.gibello.com/code/zql/

