package com.s44801165.CPEN431.A4.server;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.s44801165.CPEN431.A4.protocol.NetworkMessage;

public class Server {
    private DatagramSocket mSocket;
    private BlockingQueue<NetworkMessage> mQueue;
    private static final int SIZE_MAX_QUEUE = 256;
    private int mNumProducers = 1;
    private int mNumConsumers = 1;

    private Server(int port) throws SocketException {
        mSocket = new DatagramSocket(port);
    }
    
    private void setNumThreads(int numProducers, int numConsumers) {
        mNumProducers = numProducers;
        mNumConsumers = numConsumers;
    }

    private void runServer() {
        mQueue = new LinkedBlockingQueue<>(SIZE_MAX_QUEUE);
        for (int i = 0; i < mNumProducers; i++) {
            createMessageProducer();
        }
        
        for (int i = 0; i < mNumConsumers; i++) {
            createMessageConsumer();
        }
    }
    
    private void createMessageProducer() {
        MessageProducer prod = new MessageProducer(mSocket, mQueue);
        prod.start();
    }
    
    private void createMessageConsumer() {
        MessageConsumer cons = new MessageConsumer(mSocket, mQueue);
        cons.start();
    }

    public static void main(String[] args) {
        final String COMMAND_NUM_PRODUCERS = "--num-producers";
        final String COMMAND_NUM_CONSUMERS = "--num-consumers";
        final String COMMAND_PORT = "--port";
        
        try {
            int port = 8082;
            int numProducers = 1;
            int numConsumers = 1;
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
            System.out.println("Number of producer threads: " + numProducers);
            System.out.println("Number of consumer threads: " + numConsumers);
            Server server = new Server(port);
            server.setNumThreads(numProducers, numConsumers);
            server.runServer();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }
}
