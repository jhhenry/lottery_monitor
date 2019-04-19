# Reliablity Requirements
In this "monitor" module, the application will consistently retrieve a lottery contract's events and send them to a kafka cluster. The application should guarantee in terms of reliability that 
1. Capture all the interested events on the blockchain, never missing one.
2. Send all the captured events to the kafka server and guarantee kafka record all of them.

# Engineering requirements
These requirements above induces the following engineering requirements:
1. The application can start from any specified blocks to get interested events so that it can compensate any events it thinks missing.
2. To know whether the application missed some interested events, the application should check either by itself or through a reference data whether a event is missed.

# Configuration Requirements
## Kafka Producer Config
1. acks=all : the expected throughput of the incoming events would not be high because the interval of ethereum blocks is more than 10 seconds and a block usually contains less than 100 transactions.

## Broker Config
1. min.insync.replicas = 3: the number here should be more than the half amount of the replicas
2. unclean.leader.election.enable = false

# Current implementation
## Engineering Req. 1
1. The entry point accepts a "since" parameter to dictate the application to capture events from the "since" block.(<span style="color:green">Status: Realized</span>)

## Engineering Req. 2
There could be two solutions to resolve the requirement 2
1. After getting a event, the application will check the block where the event is within and the previous undetected blocks to look for any missed events. <br> Once it makes sure all interested event in a block is captured and sent to kafka, it should record the block number persistently to indicate the block and its previous ones have been checked.(<span style="color:green">Status: Planned</span>)
2. Build another application that independently monitors the blockchain and record the block that have interested events. The application use the data recorded by the referent application as a reference to check if there is events missing((<span style="color:green">Status: Considered</span>))
