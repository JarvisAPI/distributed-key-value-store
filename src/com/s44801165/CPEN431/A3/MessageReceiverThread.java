package com.s44801165.CPEN431.A3;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

import com.s44801165.CPEN431.A3.MessageTuple.MessageType;
import com.s44801165.CPEN431.A3.protocol.NetworkMessage;

public class MessageReceiverThread extends Thread {
    private DatagramSocket mSocket;
    private TimeoutStrategy mTimeoutStrategy;
    private BlockingQueue<MessageTuple> mQueue;
    private volatile boolean mShouldStop = false;

    /**
     * 
     * @param socket
     *            the socket to listen on.
     */
    public MessageReceiverThread(DatagramSocket socket,
            BlockingQueue<MessageTuple> queue) {
        mQueue = queue;
        mSocket = socket;
    }

    public void setTimeoutStrategy(TimeoutStrategy timeoutStrategy) {
        mTimeoutStrategy = timeoutStrategy;
    }

    public void run() {
        byte[] maxDataBuf = NetworkMessage.getMaxDataBuffer();
        DatagramPacket replyPacket = new DatagramPacket(maxDataBuf, maxDataBuf.length);
        
        if (mTimeoutStrategy != null) {
            mTimeoutStrategy.reset();
        }
        
        while (!mShouldStop) {
            MessageType type = null;
            NetworkMessage replyMessage = null;
            try {
                if (mTimeoutStrategy != null) {
                    mSocket.setSoTimeout(mTimeoutStrategy.getTimeout());
                }
                mSocket.receive(replyPacket);
                type = MessageType.MSG_RECEIVED;
                replyMessage = NetworkMessage.contructMessage(
                        Arrays.copyOf(replyPacket.getData(), replyPacket.getLength()));
                
                if (mTimeoutStrategy != null) {
                    mTimeoutStrategy.reset();
                }
            } catch (IOException e) {
                if (mTimeoutStrategy != null) {
                    mTimeoutStrategy.onTimedOut();
                }
                type = MessageType.TIMEOUT;
            } catch (Exception e) {
                type = MessageType.ERROR;
            }
            
            mQueue.add(new MessageTuple(type, replyMessage));
        }
    }
    
    public void signalStop() {
        mShouldStop = true;
    }
}
