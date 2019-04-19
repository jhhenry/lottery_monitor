# Consumer Reliability Requirements
1. The kafka consumer of the contract events should capture all the messages in kafka produced by the "monitor" application, never missing a single one.
2. Only those events that has been process and stored in a reliable datastore are considered consumed.

# Kafka Configuration Requirements
The engineering requirments indicate the a reliable and persistent database will be used to store the offsets of kafka events that has been processed by the applications. Therefore, the configs related to "offset" and "commit" is not much of use.

# Engineering Requirements
1. Store the state about whether a kafka event is persisted and completed processed in order to guarantee all kafka events are correctly handled and reduce as much duplicate events as possible.
2. The application can start from any offset specified by the startup parameters.
3. Tolerate the duplicate of a kafka event:  the same kafka event is read twice.
4. Tolerate the duplicate of a contract event: the produce could produce more than one kafka messages for the same contract event.
5. In case of any kinds of failures and shutdown, the application should know from somewhere where it should resume the work, that is the offset, of which it thinks it might have missed since last failures and should read from.

# Current Implementation
## Engineering Req. 1
Setup a table called "kafka_events_received" in a relational database to record whether a kafka event is received and processed.(<span style="color:green">Status: Realized</span>)

## Engineering Req. 2
The "start" function in the event_reader.js accepts a "offset" parameter to start reading kafka events from it.(<span style="color:green">Status: Realized</span>)

## Engineering Req. 3
The message handler in the event_handler.js checks the data in the database whether a kafka event is really handled. If yes, it will not be stored or processed again.(<span style="color:green">Status: Realized</span>)

## Engineering Req. 4
In the case that the "events" table gets a duplicate error forced by the ”txn" + "event_type" unique constraints, set the state of the corresponding record in the "kafka_events_received" table to "processed" though.  (<span style="color:green">Status: Planned</span>)

## Engineering Req. 5
Solution 1: Write an application that checks the "kafka_events_received" table at interval to record the offsets that have not been processed.  (<span style="color:green">Status: Considered</span>)
Solution 2: Write an application that query the "events" table to know which blocks are processed and query the kafka the ealiest message relating that block.  (<span style="color:green">Status: Considered</span>)