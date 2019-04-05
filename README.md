# CPEN 431 Project - Group 8A

Group ID: 8A

Verification Code: 7434D5FAB177209DE67E3B9E364DC5E6

Used Run Command: java -Xmx64m -jar A11.jar --port 64611 --node-list servers.txt --epidemic-port 60922 --replication-factor 3 --max-kvstore-size 32 --max-cache-size 10 --thread-pool-size 14 --queue-size 2512 --num-vnodes 1

Used Run Command (AWS): java -Xmx64m -jar A11.jar --port 43100 --replication-factor 1 --num-vnodes 1 --thread-pool-size 1 --max-kvstore-size 30 --max-cache-size 6

Brief Description: Refactored to use reactor pattern