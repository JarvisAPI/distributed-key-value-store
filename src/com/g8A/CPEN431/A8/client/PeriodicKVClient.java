package com.g8A.CPEN431.A8.client;

import com.g8A.CPEN431.A8.protocol.NetworkMessage;
import com.g8A.CPEN431.A8.protocol.Protocol;
import com.g8A.CPEN431.A8.protocol.Util;
import com.g8A.CPEN431.A8.server.MessageCache;
import com.g8A.CPEN431.A8.server.ReactorServer;
import com.g8A.CPEN431.A8.server.distribution.RouteStrategy.AddressHolder;
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
import java.util.TimerTask;

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
    private Map<ByteString, RequestBundle> mRequestMap;
    private MessageCache mMessageCache;
    private DatagramChannel mChannel;

    private long mElapsedTime;
    private static PeriodicKVClient mPeriodicKVClient;
    private volatile boolean mTimerTaskEnded = true;
    
    /**
     * 
     * @throws SocketException if cannot create socket for client.
     */
    private PeriodicKVClient(DatagramChannel channel) {
        mChannel = channel;
        mMessageCache = MessageCache.getInstance();
        mRequestMap = new ConcurrentHashMap<>();
    }
    

    @Override
    public void send(NetworkMessage msg, AddressHolder fromAddress) throws Exception {
        ReactorServer.getInstance().getThreadPool()
            .execute(new PeriodicKVClient.SendTask(new RequestBundle(msg, fromAddress)));
    }
    
    public static PeriodicKVClient makeInstance(DatagramChannel channel) {
        if (mPeriodicKVClient == null) {
            mPeriodicKVClient = new PeriodicKVClient(channel);
        }
        return mPeriodicKVClient;
    }
    
    public static PeriodicKVClient getInstance() {
        return mPeriodicKVClient;
    }
    
    private void checkPeriodicTask() {
        synchronized(mPeriodicKVClient) {
            if (mTimerTaskEnded) {
                mTimerTaskEnded = false;
                Util.timer.schedule(new AperiodicTask(), PERIODIC_TASK_INTERVAL);
                mElapsedTime = System.nanoTime();
            }
        }
    }
    
    public static class SendTask implements Runnable {
        private RequestBundle mRequestBundle;
        
        public SendTask(Object attachment) {
            mRequestBundle = (RequestBundle) attachment;
        }
        
        @Override
        public void run() {
            try {
                mPeriodicKVClient.mRequestMap.put(mRequestBundle.msg.getIdString(), mRequestBundle);
                mPeriodicKVClient.checkPeriodicTask();
                mPeriodicKVClient.sendPacket(mRequestBundle.msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
    }
    
    public static class ReceiveTask implements Runnable {
        private byte[] mDataBytes;
        
        public ReceiveTask(byte[] dataBytes) {
            mDataBytes = dataBytes;
        }

        @Override
        public void run() {
            NetworkMessage replyMessage;
            try {
                replyMessage = NetworkMessage.contructMessage(mDataBytes);
    
                RequestBundle requestBundle = mPeriodicKVClient.mRequestMap.remove(replyMessage.getIdString());
                // If requestBundle == null then that means we sent multiple requests due to timeout
                // but we already processed and sent the reply
                if (requestBundle != null && requestBundle.fromAddress != null) {
                    mPeriodicKVClient.mMessageCache.put(replyMessage.getIdString(),
                                      ByteString.copyFrom(replyMessage.getDataBytes()), 0, 0);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
    }
    
    private void sendPacket(NetworkMessage msg) throws IOException {
        mChannel.send(ByteBuffer.wrap(msg.getDataBytes()), new InetSocketAddress(msg.getAddress(), msg.getPort()));
    }
    
    private class AperiodicTask extends TimerTask {

        @Override
        public void run() {
            try {
                byte[] FAILED_BYTES = KVResponse.newBuilder()
                        .setErrCode(Protocol.ERR_SYSTEM_OVERLOAD)
                        .build()
                        .toByteArray();
                
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
                
                synchronized(mPeriodicKVClient) {
                    if (!mRequestMap.isEmpty()) {
                        Util.timer.schedule(new AperiodicTask(), PERIODIC_TASK_INTERVAL);
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
