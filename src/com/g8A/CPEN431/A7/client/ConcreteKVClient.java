package com.g8A.CPEN431.A7.client;

import com.g8A.CPEN431.A7.protocol.NetworkMessage;
import com.g8A.CPEN431.A7.protocol.Protocol;
import com.g8A.CPEN431.A7.server.MessageCache;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
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
    public static final int INITIAL_TIMEOUT = 400; // In milliseconds.
    public static final int MAX_RETRY_COUNT = 3;
    private static int MAX_NUM_QUEUE_ENTRIES = 1024;
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
    private static final int RECEIVE_WAIT_TIMEOUT = 10;
    private static final int POLL_TIMEOUT = 10; // Amount of time in milliseconds to wait for item to send
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
        mQueue = new LinkedBlockingQueue<>(MAX_NUM_QUEUE_ENTRIES);
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
        final long ELAPSED_RESET_TIME = INITIAL_TIMEOUT * 1000 * 1000;
        byte[] FAILED_BYTES = KVResponse.newBuilder()
                .setErrCode(Protocol.ERR_SYSTEM_OVERLOAD)
                .build()
                .toByteArray();
        NetworkMessage replyMessage = new NetworkMessage();
        long elapsedTime = 0;
        boolean resetElapsedTime = true;
        
        RequestBundle requestBundle;
        while (true) {
            if (resetElapsedTime) {
                resetElapsedTime = false;
                elapsedTime = System.nanoTime();
            }
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
                    // If requestBundle == null then that means we sent multiple requests due to timeout
                    // but we already processed and sent the reply
                    if (requestBundle != null && requestBundle.fromAddress != null) {
                        mMessageCache.put(replyMessage.getIdString(),
                                          ByteString.copyFrom(replyMessage.getDataBytes()), 0, 0);
                        /*
                        replyMessage.setAddressAndPort(requestBundle.fromAddress.address,
                                                       requestBundle.fromAddress.port);
                        
                        sendPacket(replyMessage);
                        */
                    }
                } catch (SocketTimeoutException e) {
                    // Ignore
                } catch(IOException e) {
                    e.printStackTrace();
                }
                
                if (System.nanoTime() - elapsedTime < ELAPSED_RESET_TIME) {
                    continue;
                }
                resetElapsedTime = true;
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
                            if (bundle.fromAddress != null) {
                                bundle.msg.setPayload(FAILED_BYTES);
                                bundle.msg.setAddressAndPort(bundle.fromAddress.address,
                                        bundle.fromAddress.port);
                                mMessageCache.put(bundle.msg.getIdString(),
                                        ByteString.copyFrom(bundle.msg.getDataBytes()), 0, 0);
                                sendPacket(bundle.msg);
                            }
                            it.remove();
                        }
                    }
                    else {
                        bundle.timer -= elapsedTime;
                    }
                }
            } catch(Exception e) {
                e.printStackTrace();
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
    
    public static void setMaxNumQueueEntries(int maxNumQueueEntries) {
        MAX_NUM_QUEUE_ENTRIES = maxNumQueueEntries;
    }
}
