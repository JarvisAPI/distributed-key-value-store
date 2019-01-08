package com.s44801165.CPEN431.A1;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.List;

import com.s44801165.CPEN431.A1.MessageObserver.MessageType;
import com.s44801165.CPEN431.A1.protocol.NetworkMessage;

public class MessageReceiverThread extends Thread {
    private DatagramSocket mSocket;
    private TimeoutStrategy mTimeoutStrategy;
    private List<MessageObserver> mMessageObservers;
    private volatile boolean mShouldStop = false;

    /**
     * 
     * @param socket
     *            the socket to listen on.
     */
    public MessageReceiverThread(DatagramSocket socket) {
        mMessageObservers = new ArrayList<>();
        mSocket = socket;
    }

    public void attachMessageObserver(MessageObserver observer) {
        mMessageObservers.add(observer);
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
                replyMessage = NetworkMessage.contructReplyMessage(replyPacket.getData());
                
                if (mTimeoutStrategy != null) {
                    mTimeoutStrategy.reset();
                }
            } catch (IOException e) {
                mTimeoutStrategy.onTimedOut();
                type = MessageType.TIMEOUT;
            } catch (Exception e) {
                type = MessageType.ERROR;
            }

            for (MessageObserver observer : mMessageObservers) {
                observer.update(type, replyMessage);
            }
        }
    }
    
    public void signalStop() {
        mShouldStop = true;
    }
}
