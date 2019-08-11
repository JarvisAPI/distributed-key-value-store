package com.dkvstore.client;

import com.dkvstore.NetworkMessage;
import com.dkvstore.Protocol;
import com.dkvstore.Util;
import com.dkvstore.server.MessageCache;
import com.dkvstore.server.ReactorServer;
import com.dkvstore.server.WriteEventHandler;
import com.dkvstore.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This class aims to provide (more) reliable transmission using
 * UDP while still adhering to at-most-once semantics. Therefore this
 * client retries up to the timeout limit assumed by the server.
 *
 */
public class PeriodicKVClient implements KVClient {
    private static final int INITIAL_TIMEOUT = 400; // In milliseconds.
    private static final int PERIODIC_TASK_INTERVAL = INITIAL_TIMEOUT; // In milliseconds.
    private static final int MAX_RETRY_COUNT = 3;
    private static final byte[] FAILED_BYTES = KVResponse.newBuilder()
            .setErrCode(Protocol.ERR_SYSTEM_OVERLOAD)
            .build()
            .toByteArray();
    
    private static class RequestBundle {
        private final NetworkMessage msg;
        private final AddressHolder fromAddress;
        private int timer; // Time in milliseconds.
        private int retryCounter;
        private int requestId = -1;
        
        private RequestBundle(NetworkMessage msg, AddressHolder fromAddress) {
            this.msg = msg;
            this.fromAddress = fromAddress;
            this.timer = INITIAL_TIMEOUT;
            this.retryCounter = 0;
        }
        
        private RequestBundle(NetworkMessage msg, AddressHolder fromAddress, int requestId) {
            this.msg = msg;
            this.fromAddress = fromAddress;
            this.timer = INITIAL_TIMEOUT;
            this.retryCounter = 0;
            this.requestId = requestId;
        }
    }
    
    private Map<ByteString, RequestBundle> mRequestMap;
    private MessageCache mMessageCache;
    private DatagramChannel mChannel;

    private long mElapsedTime;
    private volatile boolean mTimerTaskEnded = true;
    private OnResponseReceivedListener mResponseListener;
    
    /**
     * 
     * @throws SocketException if cannot create socket for client.
     */
    public PeriodicKVClient(DatagramChannel channel) {
        mChannel = channel;
        mMessageCache = MessageCache.getInstance();
        mRequestMap = new ConcurrentHashMap<>();
    }
    
    public DatagramChannel getChannel() {
        return mChannel;
    }
    
    @Override
    public void send(NetworkMessage msg, AddressHolder fromAddress) {
        send(new RequestBundle(msg, fromAddress));
    }
    
    @Override
    public void send(NetworkMessage msg, AddressHolder fromAddress, int requestId) {
        send(new RequestBundle(msg, fromAddress, requestId));
    }
    
    private void checkPeriodicTask() {
        synchronized(PeriodicKVClient.this) {
            if (mTimerTaskEnded) {
                mTimerTaskEnded = false;
                Util.scheduler.schedule(new AperiodicTask(), PERIODIC_TASK_INTERVAL, TimeUnit.MILLISECONDS);
                mElapsedTime = System.nanoTime();
            }
        }
    }
    
    public void setResponseListener(OnResponseReceivedListener listener) {
        mResponseListener = listener;
    }
    
    public ReceiveTask createReceiveTask(byte[] dataBytes) {
        return new ReceiveTask(dataBytes);
    }
    
    private void send(RequestBundle requestBundle) {
        try {
            mRequestMap.put(requestBundle.msg.getIdString(), requestBundle);
            checkPeriodicTask();
            sendPacket(requestBundle.msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public class ReceiveTask implements Runnable {
        private byte[] mDataBytes;
        
        public ReceiveTask(byte[] dataBytes) {
            mDataBytes = dataBytes;
        }

        @Override
        public void run() {
            NetworkMessage replyMessage;
            try {
                replyMessage = NetworkMessage.contructMessage(mDataBytes);
    
                RequestBundle requestBundle = mRequestMap.remove(replyMessage.getIdString());
                // If requestBundle == null then that means we sent multiple requests due to timeout
                // but we already processed and sent the reply
                if (requestBundle != null) {
                    if (requestBundle.fromAddress != null) {
                        mMessageCache.put(replyMessage.getIdString(),
                                          ByteString.copyFrom(replyMessage.getDataBytes()), 0, 0);
                    }
                    if (mResponseListener != null) {
                        if (requestBundle.requestId >= 0) {
                            mResponseListener.onResponseReceived(requestBundle.requestId, replyMessage);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
    }
    
    private void sendPacket(NetworkMessage msg) throws IOException, InterruptedException {
        WriteEventHandler.write(mChannel, ByteBuffer.wrap(msg.getDataBytes()), new InetSocketAddress(msg.getAddress(), msg.getPort()));
    }
    
    private class AperiodicTask implements Runnable {

        @Override
        public void run() {
            try {
                
                mElapsedTime = System.nanoTime() - mElapsedTime; 
                mElapsedTime = TimeUnit.NANOSECONDS.toMillis(mElapsedTime);
               
                Iterator<Entry<ByteString, RequestBundle>> it = mRequestMap.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<ByteString, RequestBundle> entry = it.next();
                    RequestBundle bundle = entry.getValue();
                    if (bundle.timer <= mElapsedTime) {
                        if (bundle.retryCounter <= MAX_RETRY_COUNT) {
                            bundle.retryCounter++;
                            int leftover = (int) mElapsedTime - bundle.timer;
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
                        bundle.timer -= mElapsedTime;
                    }
                }
                
                synchronized(PeriodicKVClient.this) {
                    if (!mRequestMap.isEmpty()) {
                        Util.scheduler.schedule(new AperiodicTask(), PERIODIC_TASK_INTERVAL, TimeUnit.MILLISECONDS);
                        mElapsedTime = System.nanoTime();
                    }
                    else {
                        mTimerTaskEnded = true;
                    }
                }
            } catch(Exception e) {
                e.printStackTrace();
                System.err.println("[ERROR]: ConcreteKVClient caught exception, at-most-once semantics might be broken now");
            }
        }
        
    }
}
