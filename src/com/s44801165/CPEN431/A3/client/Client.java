package com.s44801165.CPEN431.A3.client;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;

import com.s44801165.CPEN431.A3.ExponentialTimeoutStrategy;
import com.s44801165.CPEN431.A3.MessageObserver;
import com.s44801165.CPEN431.A3.MessageReceiverThread;
import com.s44801165.CPEN431.A3.protocol.NetworkMessage;
import com.s44801165.CPEN431.A3.protocol.Util;

public class Client implements MessageObserver {
    private static final int MAX_RETRY_COUNT = 3;
    private DatagramSocket mSocket;
    private DatagramPacket mSendPacket;
    private MessageReceiverThread mMsgReceiverThread;
    private int mRetryCount = 0;
    
    private void runClient(String[] args) {
        if (args.length < 3) {
            System.out.println("Not enough arguments!");
            return;
        }
        try {
            final InetAddress serverAddress = InetAddress.getByName(args[0]);
            final int serverPort = Integer.valueOf(args[1]);
            final int studentId = Integer.valueOf(args[2]);
            
            System.out.println("Sending ID: " + studentId);
            
            InetAddress clientAddress = InetAddress.getLocalHost();
            NetworkMessage msg = new NetworkMessage(Util.getUniqueId((Inet4Address) clientAddress, serverPort));
            
            byte[] dataBytes = msg.getDataBytes();
            mSendPacket = new DatagramPacket(dataBytes, dataBytes.length, serverAddress, serverPort);
            
            mSocket = new DatagramSocket();
            mSocket.send(mSendPacket);
            
            mMsgReceiverThread = new MessageReceiverThread(mSocket);
            mMsgReceiverThread.attachMessageObserver(this);
            mMsgReceiverThread.setTimeoutStrategy(new ExponentialTimeoutStrategy());
            mMsgReceiverThread.start();
            
            try {
                mMsgReceiverThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void stopMessageReceiveThread() {
        mSocket.close();
        mMsgReceiverThread.signalStop();
    }
    
    @Override
    public void update(MessageType type, NetworkMessage msg) {
        switch (type) {
        case ERROR:
            stopMessageReceiveThread();
            break;
        case TIMEOUT:
            if (mRetryCount < MAX_RETRY_COUNT) {
                try {
                    mRetryCount++;
                    mSocket.send(mSendPacket);
                } catch (IOException e) {
                    e.printStackTrace();
                    stopMessageReceiveThread();
                }
            } else {
                stopMessageReceiveThread();
            }
            break;
        case MSG_RECEIVED:
            stopMessageReceiveThread();
            break;
        default:
            break;
        }
    }
    
    public static void main(String[] args) {
        Client client = new Client();
        client.runClient(args);
    }
}
