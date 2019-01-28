package com.s44801165.CPEN431.A3.client;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

import com.s44801165.CPEN431.A3.MessageTuple;
import com.s44801165.CPEN431.A3.TimeoutStrategy;
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
            MessageTuple msgTuple = new MessageTuple();
            try {
                if (mTimeoutStrategy != null) {
                    mSocket.setSoTimeout(mTimeoutStrategy.getTimeout());
                }
                mSocket.receive(replyPacket);
                msgTuple.type = MessageType.MSG_RECEIVED;
                msgTuple.message = NetworkMessage.contructMessage(
                        Arrays.copyOf(replyPacket.getData(), replyPacket.getLength()));
                
                if (mTimeoutStrategy != null) {
                    mTimeoutStrategy.reset();
                }
            } catch (SocketTimeoutException e) {
                if (mTimeoutStrategy != null) {
                    msgTuple.timeout = mTimeoutStrategy.getTimeout();
                    mTimeoutStrategy.onTimedOut();
                }
                msgTuple.type = MessageType.TIMEOUT;
            } catch (Exception e) {
                msgTuple.type = MessageType.ERROR;
            }
            
            mQueue.add(msgTuple);
        }
    }
    
    public void signalStop() {
        mShouldStop = true;
    }
}
