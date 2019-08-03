# Distributed key-value store

Used Run Command: java -Xmx64m -jar A12.jar --port 64611 --node-list servers.txt --epidemic-port 60922 --replication-factor 3 --max-kvstore-size 32 --max-cache-size 10 --thread-pool-size 14 --queue-size 2512 --num-vnodes 1

Used Run Command (AWS): java -Xmx64m -jar A12.jar --port 43100 --replication-factor 1 --num-vnodes 1 --thread-pool-size 1 --max-kvstore-size 30 --max-cache-size 6

Brief Description: See Description.pdf for more detailed description