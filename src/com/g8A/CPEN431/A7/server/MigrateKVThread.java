package com.g8A.CPEN431.A7.server;

import java.net.Inet4Address;
import java.util.Set;

import com.g8A.CPEN431.A7.client.KVClient;
import com.g8A.CPEN431.A7.protocol.NetworkMessage;
import com.g8A.CPEN431.A7.protocol.Protocol;
import com.g8A.CPEN431.A7.protocol.Util;
import com.g8A.CPEN431.A7.server.KeyValueStore.ValuePair;
import com.g8A.CPEN431.A7.server.distribution.DirectRoute;
import com.g8A.CPEN431.A7.server.distribution.HashEntity;
import com.g8A.CPEN431.A7.server.distribution.NodeTable;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;

public class MigrateKVThread extends Thread {
	private AddressHolder mToAddress;
	private int NUM_OF_PUTS = 5;
	private int RETRY_INTERVAL = 20;

	/**
	 * Given affected ranges and the address of target node, move affected keys from current node to target node
	 * @param affectedRanges
	 * @param toAddress
	 */
    public MigrateKVThread(AddressHolder toAddress) {
    	mToAddress = toAddress;
    }

    public void run() {
        try {
        	HashEntity hashEntity = HashEntity.getInstance(); 	
        	Set<ByteString> keySet = KeyValueStore.getInstance().getKeys();
        	ValuePair vPair;
        	int tries = 0;
    
        	byte[] dataBuf;
            NetworkMessage message;
            KeyValueRequest.KVRequest.Builder kvReqBuilder = KeyValueRequest.KVRequest.newBuilder();
    
        	
            boolean keySent;
            int nodeId;
            int selfNodeId = DirectRoute.getInstance().getSelfNodeId();
            
            KVClient kvClient = Server.getInstance().getKVClient();
            Inet4Address selfAddress;
            try {
                selfAddress = (Inet4Address) NodeTable.getInstance().getSelfAddressHolder().address;
            } catch (Exception e) {
                selfAddress = (Inet4Address) Inet4Address.getLoopbackAddress();
            }
        	for(ByteString key : keySet) {
        	    nodeId = hashEntity.getKVNodeId(key);
            	
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
                if (nodeId != selfNodeId) {
        			// send put request to new node
        			vPair = KeyValueStore.getInstance().get(key);
        			
        			dataBuf = kvReqBuilder.setCommand(Protocol.PUT)
        					.setKey(key)        					
                            .setValue(vPair.value)
                            .setVersion(vPair.version)
                            .build()
                            .toByteArray();
        			
        	        keySent = false;
        	        while (!keySent) {
            			try {
    	                    message = new NetworkMessage(Util.getUniqueId(selfAddress, mToAddress.port));
    	                    message.setPayload(dataBuf);
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
            			}
        	        }
            	}
            }
        } finally {
            MessageConsumer.stopMigration();
        }
    }

    
}

