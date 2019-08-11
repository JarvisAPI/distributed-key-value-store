# Distributed key-value store
A distributed key-value store written in java that uses the reactor pattern to handle requests. For detecting node failures the epidemic protocol is used. See Description.pdf for more detailed description.

## Run Options
* --thread-pool-size *num* (Optional) Set the number of threads in the thread pool.
* --port *num* (Optional) Set the port number that the server listens on.
* --epidemic-port *num* (Optional) Set the port for the epidemic protocol to use.
* --node-list *filename* (Optional) Tells the server which file contains the node information. The file format is one line per server entry and each entry is of the format *hostname:port:epidemic_port*.
* --local-test (Optional) This option is used for testing locally, use it if the server entry provided via the node list file contains localhost or 127.0.0.1 as the hostname.
* --num-vnodes *num* (Optional) Sets the number of virtual nodes that the server uses. Currently this number must be the same accross all server instances participating in the same distributed system.
* --max-kvstore-size *num* (Optional) The approxiamte maximum number of MB that the kv-store will use before throwing out of memory error.
* --max-cache-size *num* (Optional) The approximate maximum number of MB that the message cache will use before dropping requests.
* --queue-size *num* (Optional) The maximum number of messages that the server will store in its request queue (which stores requests to other server instances).
* --replication-factor *num* (Optional) The number of replicas that the distributed system will try to store in the distributed system.

## Features
* Event driven architecture for scalability and performance
* Fault tolerant (tested in a memory contrainted and unreliable environment: planetlab)
* Reliable (replication is able to recover keys when nodes fail)

## Limitations
* Currently only supports in memory storage.
