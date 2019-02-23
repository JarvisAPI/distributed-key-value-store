package com.g8A.CPEN431.A7.server;

import java.io.IOException;
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
import com.g8A.CPEN431.A7.protocol.Protocol;
import com.g8A.CPEN431.A7.server.KeyValueStore.ValuePair;
import com.g8A.CPEN431.A7.server.distribution.HashEntity;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;

public class MigrateKVThread extends Thread {
	private volatile boolean mShouldStop = false;
	private List<long[]> mAffectedRanges;
	private AddressHolder mToAddress;
	private BlockingQueue<NetworkMessage> mBlockingQueue;
	private int NUM_OF_PUTS = 5;
	private int RETRY_INTERVAL = 20;

	/**
	 * Given affected ranges and the address of target node, move affected keys from current node to target node
	 * @param affectedRanges
	 * @param toAddress
	 */
    public MigrateKVThread(List<long[]> affectedRanges, AddressHolder toAddress, BlockingQueue<NetworkMessage> queue) {
    	mAffectedRanges = affectedRanges;
    	mToAddress = toAddress;
    	mBlockingQueue = queue;
    }

    public void run() {
    	KeyValueStore kvStore = KeyValueStore.getInstance();
    	HashEntity hashEntity = HashEntity.getInstance(); 	
    	Set<ByteString> keySet = kvStore.getKeys();
    	ValuePair vPair;
    	int tries = 0;

    	byte[] maxDataBuf;
        NetworkMessage message;
        //KeyValueResponse.KVResponse.Builder kvResBuilder = KeyValueResponse.KVResponse.newBuilder();
        KeyValueRequest.KVRequest.Builder kvReqBuilder = KeyValueRequest.KVRequest.newBuilder();

    	
    	for(ByteString key : keySet) {
        	long vIndex = hashEntity.getHashValue(key);
        	
        	if(tries == NUM_OF_PUTS) {
        		tries = 0;
        		try {
					Thread.sleep(RETRY_INTERVAL);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
        	}else {
        		tries++;
        	}
        	
        	for(long[] range : mAffectedRanges) {
        		if(vIndex >= range[0] && vIndex <= range[1]) {
        			// send put request to new node
        			vPair = kvStore.get(key);
        			
        			maxDataBuf = kvReqBuilder.setCommand(Protocol.PUT)
        					.setKey(key)        					
                            .setValue(vPair.value)
                            .setVersion(vPair.version)
                            .build()
                            .toByteArray();
        			
        			try {
	                    message = NetworkMessage
	                            .contructMessage(maxDataBuf);
	                    message.setAddressAndPort(mToAddress.address, mToAddress.port);
	                    
	                    try {
	                        mBlockingQueue.add(message);
	                    } catch (IllegalStateException e) {
	                    	
	                    	try {
								Thread.sleep(RETRY_INTERVAL);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}
	                    }
                    
        			} catch (IOException e) {
        				e.printStackTrace();
        			}
        			
        		}
        	}
        }
    }

    
}

