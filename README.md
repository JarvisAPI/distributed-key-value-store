# CPEN 431 Assignment 4
---
## How to Run
`
java -Xmx64m -jar A4.jar 8082
`

This starts the server on port 8082, if no port is supplied the server defaults to listen on 8082

---
## Tests

* At-most-once semantic test:
	* Command order: PUT -> PUT -> GET
	* Two puts with same message id and key but different value
	* Get to verify only first PUT was executed and second one returned cached result
* Get pid test:
	* Command: Get PID
	* Tests that the get pid command is working