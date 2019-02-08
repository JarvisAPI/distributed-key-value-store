package com.g8A.CPEN431.A4.client;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.g8A.CPEN431.A4.ExponentialTimeoutStrategy;
import com.g8A.CPEN431.A4.MessageTuple;
import com.g8A.CPEN431.A4.protocol.NetworkMessage;
import com.g8A.CPEN431.A4.protocol.Util;
import com.google.protobuf.ByteString;

public abstract class BaseClient {
    private static final int MAX_RETRY_COUNT = 3;
    private DatagramSocket mSocket;
    private MessageReceiverThread mMsgReceiverThread;
    private boolean mShouldStop = false;
    private static final int INITIAL_TIMEOUT = 100;
    
    private BlockingQueue<MessageTuple> mReceiveQueue = new LinkedBlockingQueue<>();
    private Map<ByteString, SendPackage> mSendPackageMap = new HashMap<>();
    
    public static class SendPackage {
        public final NetworkMessage message;
        public final DatagramPacket packet;
        public int retryCount = 0;
        public int currentTimeoutLimit = INITIAL_TIMEOUT;
        public int timeout = INITIAL_TIMEOUT;
        
        public SendPackage(NetworkMessage message, DatagramPacket packet) {
            this.message = message;
            this.packet = packet;
        }
    }
    
    protected final void sendPacket(NetworkMessage message) throws IOException {
        byte[] dataBytes = message.getDataBytes();
        DatagramPacket packet = new DatagramPacket(dataBytes, dataBytes.length,
                message.getAddress(), message.getPort());
        mSendPackageMap.put(message.getIdString(), new SendPackage(message, packet));
        mSocket.send(packet);
    }
    
    public void runClient() throws SocketException {
        mSocket = new DatagramSocket();
        mMsgReceiverThread = new MessageReceiverThread(mSocket, mReceiveQueue);
        mMsgReceiverThread.setTimeoutStrategy(new ExponentialTimeoutStrategy(INITIAL_TIMEOUT));
    }
    
    private void stopMessageReceiveThread() {
        mSocket.close();
        mMsgReceiverThread.signalStop();
    }
    
    /**
     * Wait for response messages from the server.
     */
    protected final void waitForMessages() {
        mShouldStop = false;
        try {
            if (!mMsgReceiverThread.isAlive()) {
                mMsgReceiverThread.start();
            }
            while (!mShouldStop) {
                    processMessage(mReceiveQueue.take());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Stops everything, the client shouldn't be used after a call to this
     * method.
     */
    protected final void quit() {
        mShouldStop = true;
        stopMessageReceiveThread();
        if (mMsgReceiverThread != null) {
            try {
                mMsgReceiverThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    protected final void stopWaiting() {
        mShouldStop = true;
    }
    
    private long elapsedTime = System.nanoTime();
    private final void processMessage(MessageTuple msgTuple) {
        switch (msgTuple.type) {
        case ERROR:
            System.out.println("Socket error! terminating");
            stopMessageReceiveThread();
            break;
        case TIMEOUT: {
            try {
                // TODO: Change timeout to adhere to at-most-once semantics
                System.out.println("Timeout Called: " + msgTuple.timeout);
                long time = System.nanoTime();
                long elapsedTimeNano = time - elapsedTime;
                int elapsedTimeMillis = (int) TimeUnit.NANOSECONDS.toMillis(elapsedTimeNano);
                System.out.println("Elapsed time: " + elapsedTimeMillis);
                elapsedTime = time;
                
                Iterator<Entry<ByteString, SendPackage>> it = mSendPackageMap.entrySet().iterator();
                while (it.hasNext()) {
                    SendPackage pack = it.next().getValue();
                    pack.timeout -= msgTuple.timeout;
                    if (pack.timeout <= 0) {
                        pack.retryCount++;
                        if (pack.retryCount > MAX_RETRY_COUNT) {
                            System.out.println("Retry limit reached, dropping message with ID: ");
                            Util.printHexString(pack.message.getId());
                            it.remove();
                            continue;
                        }
                        pack.currentTimeoutLimit *= 2;
                        pack.timeout = pack.currentTimeoutLimit;
                        
                        System.out.println("Timeout: " + pack.timeout + " message with ID: ");
                        Util.printHexString(pack.message.getId());
                        
                        mSocket.send(pack.packet);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                stopMessageReceiveThread();
            }
            break;
        }
        case MSG_RECEIVED:
            ByteString msgId = msgTuple.message.getIdString();
            if (mSendPackageMap.remove(msgId) != null) {
                System.out.println("Received: Message ID: ");
                Util.printHexString(msgId.toByteArray());
                onMessageReceived(msgTuple.message);
            } else {
                System.out.println("Message not in sendPackageMap: Message ID: ");
                Util.printHexString(msgId.toByteArray());
            }
            break;
        default:
            break;
        }
    }
    
    protected abstract void onMessageReceived(NetworkMessage message);
}
