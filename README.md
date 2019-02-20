# CPEN 431 Project - Group 8A

Group ID: 8A
Verification Code: 2D271EB546D5A19B28117FBFAFEC7BC7
Used Run Command: java -Xmx64m -jar A6.jar --port 50111 --max-cache-size 12 --max-kvstore-size 34 --single-thread --max-kvclient-queue-entries 8192 --node-list servers.txt

* William Ou - 44801165
* Xing Yu Tao - 33610149
* Sammie Jiang - 32157091
---
## How to Run
`
java -Xmx64m -jar A4.jar --port 8082 --num-producers 1 --num-consumers 1 --max-kvstore-size 40 --max-cache-size 8
`

This starts the server on port 8082, if no port is supplied the server defaults to listen on 8082.
All command line options are optional, the server defaults to using the values listed in this example command.

options:

* `--port`: specifies the port that the server should listen on
* `--single-thread`: cause the server to run on a single thread, if this is set then other parameters that assume multiple threads will be ignored
* `--num-producers`: sets the number of threads that are listening for network messages and placing them into a network queue for processing
* `--num-consumers`: sets the number of threads that are processing the incoming requests on the network queue
* `--max-kvstore-size`: the max size limit for the key-value store in MB
* `--max-cache-size`: the max size limit for the message cache in MB
* `--max-receive-queue-entry-limit`: the max size of the network queue in terms of number of messages, if producers are enabled

---
## Tests

* At-most-once semantic test:
	* Command order: PUT -> PUT -> GET
	* Two puts with same message id and key but different value
	* Get to verify only first PUT was executed and second one returned cached result
* Get pid test:
	* Command: Get PID
	* Tests that the get pid command is working