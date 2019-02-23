package com.g8A.CPEN431.A7.server;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.g8A.CPEN431.A7.MessageTuple;
import com.g8A.CPEN431.A7.TimeoutStrategy;
import com.g8A.CPEN431.A7.protocol.NetworkMessage;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;

public class MigrateKVThread {
	private volatile boolean mShouldStop = false;
	private List<long[]> mAffectedRanges;
	private AddressHolder mToAddress;

    public MigrateKVThread(List<long[]> affectedRanges, AddressHolder toAddress) {
    	mAffectedRanges = affectedRanges;
    	mToAddress = toAddress;
    }

    public void run() {
        while (!mShouldStop) {
            
        }
    }
    
    public void signalStop() {
        mShouldStop = true;
    }
}
