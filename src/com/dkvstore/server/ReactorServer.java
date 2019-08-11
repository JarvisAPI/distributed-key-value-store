package com.dkvstore.server;

import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.net.StandardSocketOptions;

import com.dkvstore.NetworkMessage;
import com.dkvstore.Protocol;
import com.dkvstore.client.PeriodicKVClient;
import com.dkvstore.server.WriteEventHandler.WriteBundle;
import com.dkvstore.server.distribution.DirectRoute;
import com.dkvstore.server.distribution.EpidemicProtocol;
import com.dkvstore.server.distribution.HashEntity;
import com.dkvstore.server.distribution.NodeTable;
import com.dkvstore.server.distribution.PeriodicKVCheckup;

public final class ReactorServer {
    private ExecutorService mThreadPool;
    private static ReactorServer mReactorServer;
    public static int KEY_VALUE_PORT = 50111;
    private Reactor mReactor;
    private static final String VERSION = "v2.4.12";
    
    private PeriodicKVClient mPrimaryKVClient;
    private PeriodicKVClient mSecondaryKVClient;
    
    private static int QUEUE_SIZE = 2048;
    
    private ReactorServer(int port, int threadPoolSize) throws Exception {
        KEY_VALUE_PORT = port;
        mThreadPool = Executors.newFixedThreadPool(threadPoolSize);
        
        DatagramChannel channel = DatagramChannel.open();
        channel.setOption(StandardSocketOptions.SO_SNDBUF, NetworkMessage.MAX_PAYLOAD_SIZE * 2);
        channel.socket().bind(new InetSocketAddress(KEY_VALUE_PORT));
        channel.configureBlocking(false);
        
        DatagramChannel primaryKvClientChannel = DatagramChannel.open();
        primaryKvClientChannel.setOption(StandardSocketOptions.SO_SNDBUF, NetworkMessage.MAX_PAYLOAD_SIZE * 2);
        primaryKvClientChannel.bind(null);
        primaryKvClientChannel.configureBlocking(false);
        
        mPrimaryKVClient = new PeriodicKVClient(primaryKvClientChannel);
        
        DatagramChannel secondaryKvClientChannel = DatagramChannel.open();
        secondaryKvClientChannel.setOption(StandardSocketOptions.SO_SNDBUF, NetworkMessage.MAX_PAYLOAD_SIZE * 2);
        secondaryKvClientChannel.bind(null);
        secondaryKvClientChannel.configureBlocking(false);
        
        mSecondaryKVClient = new PeriodicKVClient(secondaryKvClientChannel);
        
        MigrateKVHandler.makeInstance(mSecondaryKVClient);
        
        DatagramChannel epidemicChannel = DatagramChannel.open();
        epidemicChannel.setOption(StandardSocketOptions.SO_SNDBUF, NetworkMessage.MAX_PAYLOAD_SIZE * 2);
        epidemicChannel.bind(new InetSocketAddress(EpidemicProtocol.EPIDEMIC_SRC_PORT));
        epidemicChannel.configureBlocking(false);
        
        EpidemicProtocol.makeInstance(epidemicChannel);
        EpidemicProtocol.getInstance().start();
        
        mReactor = Reactor.makeInstance();
        mReactor.registerChannel(SelectionKey.OP_READ, channel);
        mReactor.registerChannel(SelectionKey.OP_READ, primaryKvClientChannel);
        mReactor.registerChannel(SelectionKey.OP_READ, secondaryKvClientChannel);
        mReactor.registerChannel(SelectionKey.OP_READ, epidemicChannel);
        
        channel.keyFor(mReactor.getDemultiplexer()).attach(new LinkedBlockingQueue<WriteBundle>(QUEUE_SIZE));
        primaryKvClientChannel.keyFor(mReactor.getDemultiplexer()).attach(new LinkedBlockingQueue<WriteBundle>(QUEUE_SIZE));
        secondaryKvClientChannel.keyFor(mReactor.getDemultiplexer()).attach(new LinkedBlockingQueue<WriteBundle>(QUEUE_SIZE));
        
        mReactor.registerEventHandler(SelectionKey.OP_READ, new ReadEventHandler(mThreadPool, mPrimaryKVClient, mSecondaryKVClient));
        mReactor.registerEventHandler(SelectionKey.OP_WRITE, new WriteEventHandler());
    }
    
    public ExecutorService getThreadPool() {
        return mThreadPool;
    }
    
    public PeriodicKVClient getPrimaryKVClient() {
        return mPrimaryKVClient;
    }
    
    public PeriodicKVClient getSecondaryKVClient() {
        return mSecondaryKVClient;
    }

    private void run() {
        mReactor.run();
    }
    
    public static ReactorServer getInstance() {
        return mReactorServer;
    }
    
    public static ReactorServer makeInstance(int port, int threadPoolSize) throws Exception {
        if (mReactorServer == null) {
            mReactorServer = new ReactorServer(port, threadPoolSize);
        }
        return mReactorServer;
    }
    
    public static void main(String args[]) throws Exception {
        final String COMMAND_THREAD_POOL_SIZE = "--thread-pool-size";
        final String COMMAND_PORT = "--port";
        final String COMMAND_NODE_LIST = "--node-list";
        final String COMMAND_EPIDEMIC_PORT = "--epidemic-port";
        final String COMMAND_IS_LOCAL_TEST = "--local-test";
        final String COMMAND_NUM_VNODES = "--num-vnodes";
        final String COMMAND_MAX_KV_STORE_SIZE = "--max-kvstore-size";
        final String COMMAND_MAX_CACHE_SIZE = "--max-cache-size";
        final String COMMAND_QUEUE_SIZE = "--queue-size";
        final String COMMAND_REPLICATION_FACTOR = "--replication-factor";
        
        int threadPoolSize = 2;
        int port = 50111;
        boolean isLocal = false;
        int numVNodes = 1;
        for (int i = 0; i < args.length; i += 2) {
            switch(args[i]) {
            case COMMAND_THREAD_POOL_SIZE:
                threadPoolSize = Integer.parseInt(args[i+1]);
                break;
            case COMMAND_PORT:
                port = Integer.parseInt(args[i+1]);
                ReactorServer.KEY_VALUE_PORT = port;
                break;
            case COMMAND_NODE_LIST:
                NodeTable.parseNodeListFile(args[i+1]);
                break;
            case COMMAND_EPIDEMIC_PORT:
                EpidemicProtocol.EPIDEMIC_SRC_PORT = Integer.parseInt(args[i+1]);
                break;
            case COMMAND_IS_LOCAL_TEST:
                isLocal = true;
                i -= 1;
                break;
            case COMMAND_NUM_VNODES:
                numVNodes = Integer.parseInt(args[i+1]);
                break;
            case COMMAND_MAX_KV_STORE_SIZE:
                KeyValueStore.MAX_SIZE_BYTES = Integer.parseInt(args[i+1]) * 1024 * 1024;
                break;
            case COMMAND_MAX_CACHE_SIZE:
                MessageCache.SIZE_MAX_CACHE = Integer.parseInt(args[i+1]) * 1024 * 1024;
                break;
            case COMMAND_QUEUE_SIZE:
                QUEUE_SIZE = Integer.parseInt(args[i+1]);
                break;
            case COMMAND_REPLICATION_FACTOR:
                Protocol.REPLICATION_FACTOR = Integer.parseInt(args[i+1]);
                if (Protocol.REPLICATION_FACTOR < 1) {
                    Protocol.REPLICATION_FACTOR = 1;  
                }
                break;
            default:
                System.out.println("Unknown option: " + args[i]);    
            }
        }
        
        System.out.println("Starting reactor server");
        System.out.println(String.format("***version %s***", VERSION));
        System.out.println("Number of virtual nodes: " + numVNodes);
        System.out.println("Thread pool size: " + threadPoolSize);
        System.out.println("Port: " + port);
        System.out.println("Epidemic port: " + EpidemicProtocol.EPIDEMIC_SRC_PORT);
        int kvStoreSize = KeyValueStore.MAX_SIZE_BYTES;
        kvStoreSize /= (1024 * 1024);
        System.out.println("Max key value store size: " + kvStoreSize + "MB");
        int msgCacheSize = MessageCache.SIZE_MAX_CACHE;
        msgCacheSize /= (1024 * 1024);
        System.out.println("Max message cache size: " + msgCacheSize + "MB");
        
        System.out.println("Replication factor: " + Protocol.REPLICATION_FACTOR);
        System.out.println("Queue size for kv clients: " + QUEUE_SIZE);
        
        HashEntity.setNumVNodes(numVNodes);
        NodeTable.makeInstance(isLocal);
        DirectRoute.getInstance();
        
        ReactorServer.makeInstance(port, threadPoolSize);
        
        KeyValueRequestTask.init();
        PeriodicKVCheckup.getInstance().start();
        
        ReactorServer.getInstance().run();
    }

}
