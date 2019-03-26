# CPEN 431 Project - Group 8A

Group ID: 8A

Verification Code: 856F7DA62DE5EB3F40103CF07A634BD6

Used Run Command: java -Xmx64m -jar A11.jar --port 64611 --node-list servers.txt --epidemic-port 60922 --replication-factor 3 --max-kvstore-size 32 --max-cache-size 16 --thread-pool-size 16 --queue-size 2512 --epidemic-push-interval 4000 --epidemic-node-failed-mark 8 --num-vnodes 1

Used Run Command (AWS): java -Xmx64m -jar A11.jar --port 43100 --replication-factor 1 --num-vnodes 1 --thread-pool-size 1 --max-kvstore-size 30 --max-cache-size 6

Brief Description: Refactored to use reactor pattern