package com.g8A.CPEN431.A8.server;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.g8A.CPEN431.A8.client.ConcreteKVClient;
import com.g8A.CPEN431.A8.client.KVClient;
import com.g8A.CPEN431.A8.protocol.NetworkMessage;
import com.g8A.CPEN431.A8.server.distribution.DirectRoute;
import com.g8A.CPEN431.A8.server.distribution.EpidemicProtocol;
import com.g8A.CPEN431.A8.server.distribution.HashEntity;
import com.g8A.CPEN431.A8.server.distribution.NodeTable;

public class Server {
    private DatagramSocket mSocket;
    private BlockingQueue<NetworkMessage> mQueue;
    private ConcreteKVClient mKVClient;
    private static int SIZE_MAX_QUEUE = 256;
    private int mNumProducers = 1;
    private int mNumConsumers = 1;
   
    private boolean mIsSingleThread = false;
    public final int PORT;
    private static Server mServer;

    private Server(int port) throws SocketException {
        PORT = port;
        mSocket = new DatagramSocket(port);
    }
    
    private void setNumThreads(int numProducers, int numConsumers) {
        mNumProducers = numProducers;
        mNumConsumers = numConsumers;
    }
    
    private void startKVClient() {
    	try {
			mKVClient = new ConcreteKVClient();
		} catch (SocketException e) {
			e.printStackTrace();
			System.err.println("[ERROR]: Fatal, unable to open socket for KVClient");
			System.exit(1);
		}
        new Thread(mKVClient).start();
    }
    
    private void startEpidemicProtocol() throws SocketException {
        new Thread(EpidemicProtocol.makeInstance()).start();
    }

    private void runServer() throws SocketException {
        if (!mIsSingleThread) {
            mQueue = new LinkedBlockingQueue<>(SIZE_MAX_QUEUE);
            
            for (int i = 0; i < mNumProducers; i++) {
                createMessageProducer();
            }
            
            for (int i = 0; i < mNumConsumers; i++) {
                createMessageConsumer();
            }
        }
        else {
            createMessageConsumer();
        }
    }
    
    private void createMessageProducer() {
        MessageProducer prod = new MessageProducer(mSocket, mQueue);
        prod.start();
    }
    
    private void createMessageConsumer() throws SocketException {
        NetworkQueue queue;
        if (mIsSingleThread) {
            queue = new NetworkQueue() {
                private byte[] maxDataBuf = NetworkMessage.getMaxDataBuffer();
                private DatagramPacket packet = new DatagramPacket(maxDataBuf, maxDataBuf.length);
                
                @Override
                public NetworkMessage take() throws Exception {
                    mSocket.receive(packet);
                    NetworkMessage message = NetworkMessage
                            .contructMessage(Arrays.copyOf(packet.getData(), packet.getLength()));
                    message.setAddressAndPort(packet.getAddress(), packet.getPort());
                    return message;
                }
            };
        }
        else {
            queue = new NetworkQueue() {
                @Override
                public NetworkMessage take() throws Exception {
                    return mQueue.take();
                }
            };
        }
        int selfNodeId = DirectRoute.getInstance().getSelfNodeId();

        DatagramSocket socket = new DatagramSocket();
        MessageConsumer cons = new MessageConsumer(socket, queue, mKVClient, selfNodeId);
        cons.start();
    }
    
    private void setSingleThread(boolean singleThread) {
        mIsSingleThread = singleThread;
    }
    
    public static void makeInstance(int port) throws SocketException {
        if (mServer == null) {
            mServer = new Server(port);
        }
    }
    
    public static Server getInstance() {
        return mServer;
    }
    
    public KVClient getKVClient() {
        return mKVClient;
    }

    public static void main(String[] args) {
        final String COMMAND_NUM_PRODUCERS = "--num-producers";
        final String COMMAND_NUM_CONSUMERS = "--num-consumers";
        final String COMMAND_PORT = "--port";
        final String COMMAND_MAX_KEY_VALUE_STORE_SIZE = "--max-kvstore-size";
        final String COMMAND_MAX_MESSAGE_CACHE_SIZE = "--max-cache-size";
        final String COMMAND_SINGLE_THREAD = "--single-thread";
        final String COMMAND_MAX_RECEIVE_QUEUE = "--max-receive-queue-entry-limit";
        final String COMMAND_NODE_LIST = "--node-list";
        final String COMMAND_MAX_KV_CLIENT_QUEUE_ENTRIES = "--max-kvclient-queue-entries";
        // The number of virtual nodes to give to this node and the number of virtual nodes
        // that this node expects for every other node.
        final String COMMAND_NUM_VNODES = "--num-vnodes";
        final String COMMAND_EPIDEMIC_PORT = "--epidemic-port";
        final String COMMAND_EPIDEMIC_PUSH_INTERVAL = "--epidemic-push-interval";
        final String COMMAND_EPIDEMIC_NODE_FAILED_MARK = "--epidemic-node-failed-mark";
        final String COMMAND_LOCAL_TEST = "--local-test";
        try {
            int port = 8082;
            int numProducers = 1;
            int numConsumers = 1;
            int maxKeyValueStoreSize = 40;
            int maxCacheSize = 8;
            int maxReceiveQueueEntryLimit = 256;
            boolean isSingleThread = false;
            int maxKvClientQueueEntries = 1024;
            int numVNodes = 1;
            boolean isLocal = false;
            for (int i = 0; i < args.length; i+=2) {
                try {
                    switch(args[i]) {
                    case COMMAND_NUM_PRODUCERS:
                        numProducers = Integer.parseInt(args[i+1]);
                        break;
                    case COMMAND_NUM_CONSUMERS:
                        numConsumers = Integer.parseInt(args[i+1]);
                        break;
                    case COMMAND_PORT:
                        port = Integer.parseInt(args[i+1]);
                        break;
                    case COMMAND_MAX_KEY_VALUE_STORE_SIZE:
                        maxKeyValueStoreSize = Integer.parseInt(args[i+1]);
                        break;
                    case COMMAND_MAX_MESSAGE_CACHE_SIZE:
                        maxCacheSize = Integer.parseInt(args[i+1]);
                        break;
                    case COMMAND_SINGLE_THREAD:
                        i -= 1;
                        isSingleThread = true;
                        break;
                    case COMMAND_MAX_RECEIVE_QUEUE:
                        maxReceiveQueueEntryLimit = Integer.parseInt(args[i+1]);
                        break;
                    case COMMAND_NODE_LIST:
                        NodeTable.parseNodeListFile(args[i+1]);
                        break;
                    case COMMAND_MAX_KV_CLIENT_QUEUE_ENTRIES:
                        maxKvClientQueueEntries = Integer.parseInt(args[i+1]);
                        break;
                    case COMMAND_NUM_VNODES:
                        numVNodes = Integer.parseInt(args[i+1]);
                        break;
                    case COMMAND_EPIDEMIC_PORT:
                        EpidemicProtocol.EPIDEMIC_SRC_PORT = Integer.parseInt(args[i+1]);
                        break;
                    case COMMAND_EPIDEMIC_PUSH_INTERVAL:
                        EpidemicProtocol.MIN_PUSH_INTERVAL = Integer.parseInt(args[i+1]);
                        break;
                    case COMMAND_EPIDEMIC_NODE_FAILED_MARK:
                        EpidemicProtocol.NODE_HAS_FAILED_MARK = Integer.parseInt(args[i+1]);
                        break;
                    case COMMAND_LOCAL_TEST:
                        i -= 1;
                        isLocal = true;
                        break;
                    default:
                        System.out.println("Unknown option: " + args[i]);  
                    }
                } catch(Exception e) {
                    e.printStackTrace();
                    System.out.println(String.valueOf(i) + "th option ignored");
                }
            }
            System.out.println("Starting server!");
            System.out.println("Port: " + port);
            if (isSingleThread) {
                System.out.println("Server running on single thread");
            }
            else {
                System.out.println("Max receive queue entry limit: " + maxReceiveQueueEntryLimit);
                System.out.println("Number of producer threads: " + numProducers);
                System.out.println("Number of consumer threads: " + numConsumers);
            }
            System.out.println("Max key-value store size: " + maxKeyValueStoreSize + "MB");
            System.out.println("Max cache size: " + maxCacheSize + "MB");
            System.out.println("Max KV client queue entries: " + maxKvClientQueueEntries);
            System.out.println("Number of virtual nodes: " + numVNodes);
            System.out.println("Epidemic port: " + EpidemicProtocol.EPIDEMIC_SRC_PORT);
            System.out.println("Epidemic push interval: " + EpidemicProtocol.MIN_PUSH_INTERVAL + "ms");
            System.out.println("Epidemic node failed mark: " + EpidemicProtocol.NODE_HAS_FAILED_MARK);
            if (isLocal) {
                System.out.println("***Running as local test***");
            }
            
            maxKeyValueStoreSize *= 1024*1024;
            maxCacheSize *= 1024*1024;
            
            MessageCache.setMaxCacheSize(maxCacheSize);
            KeyValueStore.setMaxCacheSize(maxKeyValueStoreSize);
            ConcreteKVClient.setMaxNumQueueEntries(maxKvClientQueueEntries);
            
            HashEntity.setNumVNodes(numVNodes);
            
            new Thread(MigrateKVThread.getInstance()).start();
            Server.makeInstance(port);
            NodeTable.makeInstance(isLocal);
            Server server = Server.getInstance();
            if (!isSingleThread) {
                Server.SIZE_MAX_QUEUE = maxReceiveQueueEntryLimit;
                server.setNumThreads(numProducers, numConsumers);
            }
            else {
                server.setSingleThread(true);
            }
            server.startKVClient();
            server.startEpidemicProtocol();
            server.runServer();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("[ERROR]: Failed to startup exiting");
            System.exit(1);
        }
    }
}