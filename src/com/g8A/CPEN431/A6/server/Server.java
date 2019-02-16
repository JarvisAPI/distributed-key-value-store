package com.g8A.CPEN431.A6.server;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.g8A.CPEN431.A6.client.ConcreteKVClient;
import com.g8A.CPEN431.A6.client.KVClient;
import com.g8A.CPEN431.A6.protocol.NetworkMessage;
import com.g8A.CPEN431.A6.protocol.Util;
import com.g8A.CPEN431.A6.server.distribution.HashEntity;

public class Server {
    private DatagramSocket mSocket;
    private BlockingQueue<NetworkMessage> mQueue;
    private ConcreteKVClient mKVClient;
    private HashEntity mHashEntity;
    private static int SIZE_MAX_QUEUE = 256;
    private int mNumProducers = 1;
    private int mNumConsumers = 1;
    private int mNodeId;
    private boolean mIsSingleThread = false;

    private Server(int port) throws SocketException {
        mSocket = new DatagramSocket(port);
    }
    
    private void setNumThreads(int numProducers, int numConsumers) {
        mNumProducers = numProducers;
        mNumConsumers = numConsumers;
    }
    
    private void setHashEntityAndNodeId(int port) {
    	mHashEntity = new HashEntity();
    	InetAddress localHost;
		try {
			localHost = InetAddress.getLocalHost();
			try {
				mNodeId = mHashEntity.getKVNodeId(Util.concatHostnameAndPort(localHost.getHostAddress(), port));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
    }
    
    private void startKVClient() {
    	try {
			mKVClient = new ConcreteKVClient();
		} catch (SocketException e) {
			e.printStackTrace();
		}
        new Thread(mKVClient).start();
    }

    private void runServer() {
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
    
    private void createMessageConsumer() {
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
        MessageConsumer cons = new MessageConsumer(mSocket, queue, mKVClient, mHashEntity, mNodeId);
        cons.start();
    }
    
    private void setSingleThread(boolean singleThread) {
        mIsSingleThread = singleThread;
    }

    public static void main(String[] args) {
        final String COMMAND_NUM_PRODUCERS = "--num-producers";
        final String COMMAND_NUM_CONSUMERS = "--num-consumers";
        final String COMMAND_PORT = "--port";
        final String COMMAND_MAX_KEY_VALUE_STORE_SIZE = "--max-kvstore-size";
        final String COMMAND_MAX_MESSAGE_CACHE_SIZE = "--max-cache-size";
        final String COMMAND_SINGLE_THREAD = "--single-thread";
        final String COMMAND_MAX_RECEIVE_QUEUE = "--max-receive-queue-entry-limit";
        
        try {
            int port = 8082;
            int numProducers = 1;
            int numConsumers = 1;
            int maxKeyValueStoreSize = 40;
            int maxCacheSize = 8;
            int maxReceiveQueueEntryLimit = 256;
            int nodeId = 0;
            boolean isSingleThread = false;
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
            
            maxKeyValueStoreSize *= 1024*1024;
            maxCacheSize *= 1024*1024;
            
            MessageCache.setMaxCacheSize(maxCacheSize);
            KeyValueStore.setMaxCacheSize(maxKeyValueStoreSize);
            
            Server server = new Server(port);
            server.setHashEntityAndNodeId(port);
            if (!isSingleThread) {
                Server.SIZE_MAX_QUEUE = maxReceiveQueueEntryLimit;
                server.setNumThreads(numProducers, numConsumers);
            }
            else {
                server.setSingleThread(true);
            }
            server.startKVClient();
            server.runServer();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }
}
