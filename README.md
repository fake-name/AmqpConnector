# AqmpConnector

This is a very minimal library intended for distributed task systems, where the 
AMQP system is used for both task distribution, *and* collation of the task 
results. This seems to be somewhat unusual among most AMQP libraries, where
a AQMP client is either a pure producer, or a pure consumer. In this case,
each client consumes from one queue, and produces on another.

At it's core, this basically just allows one to put byte-strings into one end,
they're passed out to the clients, and the clients then return byte-strings,
which come out the other end.

Correlation between submitted tasks and responses is entirely left up to the 
responsibility of the end-user code.

Requires:   

 - `amqp` library