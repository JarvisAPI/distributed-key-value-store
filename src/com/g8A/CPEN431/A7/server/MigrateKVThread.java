package com.g8A.CPEN431.A7.server;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import com.g8A.CPEN431.A7.MessageTuple;
import com.g8A.CPEN431.A7.TimeoutStrategy;
import com.g8A.CPEN431.A7.protocol.NetworkMessage;
import com.g8A.CPEN431.A7.server.distribution.HashEntity;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

public class MigrateKVThread {
	private volatile boolean mShouldStop = false;
	private List<long[]> mAffectedRanges;
	private AddressHolder mToAddress;

	/**
	 * Given affected ranges and the address of target node, move affected keys from current node to target node
	 * @param affectedRanges
	 * @param toAddress
	 */
    public MigrateKVThread(List<long[]> affectedRanges, AddressHolder toAddress) {
    	mAffectedRanges = affectedRanges;
    	mToAddress = toAddress;
    }

    public void run() {
    	HashEntity hashEntity = HashEntity.getInstance(); 	
    	Set<ByteString> keySet = KeyValueStore.getInstance().getKeys();
    	
    	for(ByteString key : keySet) {
        	long vIndex = hashEntity.getHashValue(key);
        	
        	for(long[] range : mAffectedRanges) {
        		if(vIndex >= range[0] && vIndex <= range[1]) {
        			// send put request to new node
        			
        		}
        	}
        }
    }

    
}

