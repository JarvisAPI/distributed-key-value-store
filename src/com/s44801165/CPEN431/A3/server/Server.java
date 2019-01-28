package com.s44801165.CPEN431.A3.server;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.s44801165.CPEN431.A3.protocol.NetworkMessage;

public class Server {
    private DatagramSocket mSocket;
    private BlockingQueue<NetworkMessage> mQueue;
    private static final int SIZE_MAX_QUEUE = 1024;

    private Server(int port) throws SocketException {
        mSocket = new DatagramSocket(port);
    }

    private void runServer() {
        mQueue = new LinkedBlockingQueue<>(SIZE_MAX_QUEUE);
        createMessageProducer();
        
        createMessageConsumer();
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
        try {
            int port = 8082;
            if (args.length > 0) {
                try {
                    int p = Integer.parseInt(args[0]);
                    port = p;
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Starting server!");
            Server server = new Server(port);
            server.runServer();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }
}
