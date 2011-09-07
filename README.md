Guzzler: Stream MySQL binary logs and act on them
=================================================

Guzzler allows you to stream MySQL binary logs from a master and lets you
act on them using Scala actors (consumers). Consumers are configurable in
guzzler.conf along with the rest of the required parameters. Included with
Guzzler is a RabbitMQ consumer that will push queries into a RabbitMQ server
for consumption.

Guzzler can start, stop, restart and seek in the binary logs via a remote
SSH interface.  Guzzler handles connection issues or random disconnects and 
attempts to pick up where it left off.

Consumers either in Guzzler itself of behind RabbitMQ can analyze the
queries (Guzzler provides an SQL query parser based on JSqlParser) and
may decide to update counters, fire off events, log messages, etc.

Sample SSH session (no auth yet):

    -=[ Welcome to Guzzler ]=-
    
    guzzler stream stop
    guzzler stream seek db-bin.000924 900993280
		guzzler stream stop
		guzzler stream resume

JSqlParser: http://sourceforge.net/projects/jsqlparser/
