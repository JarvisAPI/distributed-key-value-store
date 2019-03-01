package com.g8A.CPEN431.A8.server;

import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.net.StandardSocketOptions;

import com.g8A.CPEN431.A8.client.KVClient;
import com.g8A.CPEN431.A8.client.PeriodicKVClient;
import com.g8A.CPEN431.A8.protocol.NetworkMessage;
import com.g8A.CPEN431.A8.server.distribution.DirectRoute;
import com.g8A.CPEN431.A8.server.distribution.EpidemicProtocol;
import com.g8A.CPEN431.A8.server.distribution.NodeTable;

public final class ReactorServer {
    private ExecutorService mThreadPool;
    private KVClient mKVClient;
    private static ReactorServer mReactorServer;
    private int mKeyValuePort;
    
    private ReactorServer(int port, int threadPoolSize) {
        mKeyValuePort = port;
        mThreadPool = Executors.newFixedThreadPool(threadPoolSize);
    }
    
    public ExecutorService getThreadPool() {
        return mThreadPool;
    }
    
    public KVClient getKVClient() {
        return mKVClient;
    }
    
    public int getKeyValuePort() {
        return mKeyValuePort;
    }

    private void run() throws Exception {
        DatagramChannel channel = DatagramChannel.open();
        channel.setOption(StandardSocketOptions.SO_SNDBUF, NetworkMessage.MAX_PAYLOAD_SIZE * 2);
        channel.socket().bind(new InetSocketAddress(mKeyValuePort));
        channel.configureBlocking(false);
        
        DatagramChannel kvClientChannel = DatagramChannel.open();
        kvClientChannel.setOption(StandardSocketOptions.SO_SNDBUF, NetworkMessage.MAX_PAYLOAD_SIZE * 2);
        kvClientChannel.bind(null);
        kvClientChannel.configureBlocking(false);
        
        mKVClient = PeriodicKVClient.makeInstance(kvClientChannel);
        
        Reactor reactor = Reactor.makeInstance();
        reactor.registerChannel(SelectionKey.OP_READ, channel);
        reactor.registerChannel(SelectionKey.OP_READ, kvClientChannel);
        
        reactor.registerEventHandler(SelectionKey.OP_READ, new ReadEventHandler());
        reactor.run();
    }
    
    public static ReactorServer getInstance() {
        return mReactorServer;
    }
    
    public static ReactorServer makeInstance(int port, int threadPoolSize) {
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
        
        int threadPoolSize = 2;
        int port = 50111;
        for (int i = 0; i < args.length; i += 2) {
            switch(args[i]) {
            case COMMAND_THREAD_POOL_SIZE:
                threadPoolSize = Integer.parseInt(args[i+1]);
                break;
            case COMMAND_PORT:
                port = Integer.parseInt(args[i+1]);
                Server.PORT = port;
                break;
            case COMMAND_NODE_LIST:
                NodeTable.parseNodeListFile(args[i+1]);
                break;
            case COMMAND_EPIDEMIC_PORT:
                EpidemicProtocol.EPIDEMIC_SRC_PORT = Integer.parseInt(args[i+1]);
                break;
            default:
                System.out.println("Unknown option: " + args[i]);    
            }
        }
        
        System.out.println("Starting reactor server");
        System.out.println("Thread pool size: " + threadPoolSize);
        System.out.println("Port: " + port);
        System.out.println("Epidemic port: " + EpidemicProtocol.EPIDEMIC_SRC_PORT);
        int kvStoreSize = KeyValueStore.MAX_SIZE_BYTES;
        kvStoreSize /= (1024 * 1024);
        System.out.println("Max key value store size: " + kvStoreSize + "MB");
        int msgCacheSize = MessageCache.SIZE_MAX_CACHE;
        msgCacheSize /= (1024 * 1024);
        System.out.println("Max message cache size: " + msgCacheSize + "MB");
        
        NodeTable.makeInstance(true);
        DirectRoute.getInstance();
        
        new Thread(EpidemicProtocol.makeInstance()).start();
        ReactorServer.makeInstance(port, threadPoolSize).run();
    }

}
