package com.g8A.CPEN431.A7.server;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.g8A.CPEN431.A7.client.KVClient;
import com.g8A.CPEN431.A7.protocol.NetworkMessage;
import com.g8A.CPEN431.A7.protocol.Protocol;
import com.g8A.CPEN431.A7.server.KeyValueStore.ValuePair;
import com.g8A.CPEN431.A7.server.distribution.HashEntity;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;

public class MigrateKVThread extends Thread {
	private List<long[]> mAffectedRanges;
	private AddressHolder mToAddress;
	private int NUM_OF_PUTS = 5;
	private int RETRY_INTERVAL = 20;

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
        try {
        	HashEntity hashEntity = HashEntity.getInstance(); 	
        	Set<ByteString> keySet = KeyValueStore.getInstance().getKeys();
        	ValuePair vPair;
        	int tries = 0;
    
        	byte[] maxDataBuf;
            NetworkMessage message;
            KeyValueRequest.KVRequest.Builder kvReqBuilder = KeyValueRequest.KVRequest.newBuilder();
    
        	
            boolean keySent;
        	for(ByteString key : keySet) {
            	long vIndex = hashEntity.getHashValue(key);
            	
            	if (tries == NUM_OF_PUTS) {
            		tries = 0;
                    try {
                        Thread.sleep(RETRY_INTERVAL);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
            	} else {
            		tries++;
            	}
                
            	KVClient kvClient = Server.getInstance().getKVClient();
            	for(long[] range : mAffectedRanges) {
            		if(vIndex >= range[0] && vIndex <= range[1]) {
            			// send put request to new node
            			vPair = KeyValueStore.getInstance().get(key);
            			
            			maxDataBuf = kvReqBuilder.setCommand(Protocol.PUT)
            					.setKey(key)        					
                                .setValue(vPair.value)
                                .setVersion(vPair.version)
                                .build()
                                .toByteArray();
            			
            	        keySent = false;
            	        while (!keySent) {
                			try {
        	                    message = NetworkMessage
        	                            .contructMessage(maxDataBuf);
        	                    message.setAddressAndPort(mToAddress.address, mToAddress.port);
        	                    kvClient.send(message, null);
        	                    keySent = true;
        	                    KeyValueStore.getInstance().remove(key);
                			} catch (IllegalStateException e) {
                                try {
                                    Thread.sleep(RETRY_INTERVAL);
                                } catch (InterruptedException e1) {
                                    // Ignored
                                }
                			} catch (IOException e) {
                			    keySent = true;
                				System.err.println("[WARNING]: Protobuf is incorrect format, this should not happen");
                			}
            	        }
            		}
            	}
            }
        } finally {
            MessageConsumer.stopMigration();
        }
    }

    
}

