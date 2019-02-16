package com.g8A.CPEN431.A6.client;

import com.g8A.CPEN431.A6.protocol.NetworkMessage;
import com.g8A.CPEN431.A6.protocol.Protocol;
import com.g8A.CPEN431.A6.server.MessageCache;
import com.g8A.CPEN431.A6.server.distribution.RouteStrategy.AddressHolder;

import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This class aims to provide (more) reliable transmission using
 * UDP while still adhering to at-most-once semantics. Therefore this
 * client retries up to the timeout limit assumed by the server.
 *
 */
public class ConcreteKVClient implements KVClient, Runnable {
    public static final int INITIAL_TIMEOUT = 100; // In milliseconds.
    public static final int MAX_RETRY_COUNT = 3;
    private static class RequestBundle {
        private final NetworkMessage msg;
        private final AddressHolder fromAddress;
        private int timer; // Time in milliseconds.
        private int retryCounter;
        
        private RequestBundle(NetworkMessage msg, AddressHolder fromAddress) {
            this.msg = msg;
            this.fromAddress = fromAddress;
            this.timer = INITIAL_TIMEOUT;
            this.retryCounter = 0;
        }
    }
    private static final int RECEIVE_WAIT_TIMEOUT = 100;
    private static final int POLL_TIMEOUT = 100; // Amount of time in milliseconds to wait for item to send
    private BlockingQueue<RequestBundle> mQueue;
    private Map<ByteString, RequestBundle> mRequestMap;
    private MessageCache mMessageCache;
    private DatagramSocket mSocket;
    private DatagramPacket mSendPacket;
    private DatagramPacket mReceivePacket;
    
    /**
     * 
     * @throws SocketException if cannot create socket for client.
     */
    public ConcreteKVClient() throws SocketException {
        mMessageCache = MessageCache.getInstance();
        mSocket = new DatagramSocket();
        mQueue = new LinkedBlockingQueue<>(64);
        mRequestMap = new HashMap<>();
        mSendPacket = new DatagramPacket(new byte[0], 0);
        byte[] dataBuf = NetworkMessage.getMaxDataBuffer();
        mReceivePacket = new DatagramPacket(dataBuf, dataBuf.length);
        
        mSocket.setSoTimeout(RECEIVE_WAIT_TIMEOUT);
    }
    

    @Override
    public void send(NetworkMessage msg, AddressHolder fromAddress) throws IllegalStateException {
        mQueue.add(new RequestBundle(msg, fromAddress));
    }

    @Override
    public void run() {
        byte[] FAILED_BYTES = KVResponse.newBuilder()
                .setErrCode(Protocol.ERR_INTERNAL_KVSTORE_FAILURE)
                .build()
                .toByteArray();
        NetworkMessage replyMessage = new NetworkMessage();
        long elapsedTime = 0;
        
        RequestBundle requestBundle;
        while (true) {
            elapsedTime = System.nanoTime();
            try {
                try {
                    requestBundle = mQueue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
                    if (requestBundle != null) {
                        mRequestMap.put(requestBundle.msg.getIdString(), requestBundle);
                        sendPacket(requestBundle.msg);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                
                try {
                    mSocket.receive(mReceivePacket);
                    NetworkMessage.setMessage(replyMessage, Arrays.copyOf(mReceivePacket.getData(),
                                                                          mReceivePacket.getLength()));
                    requestBundle = mRequestMap.remove(replyMessage.getIdString());
                    mMessageCache.put(replyMessage.getIdString(),
                                      ByteString.copyFrom(replyMessage.getDataBytes()), 0, 0);
                    
                    replyMessage.setAddressAndPort(InetAddress.getByName(requestBundle.fromAddress.hostname),
                                                   requestBundle.fromAddress.port);
                    sendPacket(replyMessage);
                } catch(IOException e) {
                    e.printStackTrace();
                }
                
                
                elapsedTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - elapsedTime);
                
                Iterator<Entry<ByteString, RequestBundle>> it = mRequestMap.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<ByteString, RequestBundle> entry = it.next();
                    RequestBundle bundle = entry.getValue();
                    if (bundle.timer <= elapsedTime) {
                        if (bundle.retryCounter <= MAX_RETRY_COUNT) {
                            bundle.retryCounter++;
                            int leftover = (int) elapsedTime - bundle.timer;
                            bundle.timer = INITIAL_TIMEOUT * (1 << bundle.retryCounter) - leftover;
                            try {
                                sendPacket(bundle.msg);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        else {
                            replyMessage.setPayload(FAILED_BYTES);
                            replyMessage.setAddressAndPort(InetAddress.getByName(bundle.fromAddress.hostname),
                                    bundle.fromAddress.port);
                            mMessageCache.put(bundle.msg.getIdString(),
                                    ByteString.copyFrom(replyMessage.getDataBytes()), 0, 0);
                            sendPacket(replyMessage);
                            it.remove();
                        }
                    }
                    else {
                        bundle.timer -= elapsedTime;
                    }
                }
            } catch(Exception e) {
                System.err.println("[ERROR]: ConcreteKVClient caught exception, at-most-once semantics might be broken now");
            }
        }
    }
    
    private void sendPacket(NetworkMessage msg) throws IOException {
        mSendPacket.setData(msg.getDataBytes());
        mSendPacket.setAddress(msg.getAddress());
        mSendPacket.setPort(msg.getPort());
        mSocket.send(mSendPacket);
    }
}
