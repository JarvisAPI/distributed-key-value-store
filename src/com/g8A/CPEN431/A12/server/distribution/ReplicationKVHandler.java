package com.g8A.CPEN431.A12.server.distribution;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.g8A.CPEN431.A12.client.KVClient;
import com.g8A.CPEN431.A12.protocol.NetworkMessage;
import com.g8A.CPEN431.A12.protocol.Protocol;
import com.g8A.CPEN431.A12.protocol.Util;
import com.g8A.CPEN431.A12.server.KeyValueStore;
import com.g8A.CPEN431.A12.server.ReactorServer;
import com.g8A.CPEN431.A12.server.KeyValueStore.ValuePair;
import com.g8A.CPEN431.A12.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;

/**
 * Handles replication on node failures.
 *
 */
public class ReplicationKVHandler {
    private int NUM_OF_PUTS = 100;
    private int RETRY_INTERVAL = 100;
    private int BATCH_INTERVAL = 10000;
    private Set<VirtualNode> mAffectedVNodes;
    private static ReplicationKVHandler mReplicationKVHandler;
    private boolean mBatchTimerStarted = false;
    
    private ReplicationKVHandler() {
        mAffectedVNodes = new HashSet<>();
    }
    
    public static synchronized ReplicationKVHandler getInstance() {
        if (mReplicationKVHandler == null) {
            mReplicationKVHandler = new ReplicationKVHandler();
        }
        return mReplicationKVHandler;
    }
    
    public void replicateToSuccessors(Set<VirtualNode> replicateVNodes) {
        synchronized(mAffectedVNodes) {
            if (!mBatchTimerStarted) {
                mBatchTimerStarted = true;
                Util.scheduler.schedule(new ReplicateToSuccessorTask(), BATCH_INTERVAL, TimeUnit.MILLISECONDS);
            }
            mAffectedVNodes.addAll(replicateVNodes);
        }
    }
    
    private class ReplicateToSuccessorTask implements Runnable {

        @Override
        public void run() {
            try {
                Set<VirtualNode> affectedVNodes;
                synchronized(mAffectedVNodes) {
                    mBatchTimerStarted = false;
                    affectedVNodes = new HashSet<>(mAffectedVNodes);
                    mAffectedVNodes.clear();
                }
                
                KeyValueRequest.KVRequest.Builder kvReqBuilder = KeyValueRequest.KVRequest.newBuilder();
    
                int[] successorNodeIds = new int[Protocol.REPLICATION_FACTOR - 1];
                
                int debugReplicationSize = 0;
                int debugNumKeysReplicated = 0;
                int tries = 0;
                
                kvReqBuilder
                    .setCommand(Protocol.PUT)
                    .setIsReplica(true);
                
                RouteStrategy routeStrat = DirectRoute.getInstance();
                KeyValueStore kvStore = KeyValueStore.getInstance();
                KVClient kvClient = ReactorServer.getInstance().getSecondaryKVClient();
                
                Set<ByteString> keys = kvStore.getKeys();
                System.out.println(String.format("[DEBUG]: replicate to successor task, looping through %d keys", keys.size()));
                
                HashEntity hashEntity = HashEntity.getInstance();
                // scan keys, replicate keys which hash to self.
                for(ByteString key : keys) {
                    VirtualNode vnode = hashEntity.getKVNode(key);
                    if (affectedVNodes.contains(vnode)) {
                        
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
                        
                        ValuePair value = kvStore.get(key);
                        kvReqBuilder
                            .setKey(key)
                            .setValue(value.value)
                            .setVersion(value.version)
                            .setSequenceStamp(value.sequenceStamp);
                        
                        byte[] payload = kvReqBuilder.build().toByteArray();
                        
                        debugReplicationSize += value.value.size();
                        debugNumKeysReplicated++;
                        
                        int numSuccessors = hashEntity.getSuccessorNodes(vnode, Protocol.REPLICATION_FACTOR - 1, successorNodeIds);
                        
                        for (int i = 0; i < numSuccessors; i++) {
                            AddressHolder replicaAddress = routeStrat.getRoute(successorNodeIds[i]);
                            NetworkMessage msg = new NetworkMessage(Util.getUniqueId(ReactorServer.KEY_VALUE_PORT));
        
                            msg.setPayload(payload);
                            msg.setAddressAndPort(replicaAddress.address, replicaAddress.port);
                            
                            kvClient.send(msg, null);
                        }
                    }
                }
                
                System.out.println(String.format("[DEBUG]: replicate to successor task, total replication size: %d", debugReplicationSize));
                System.out.println(String.format("[DEBUG]: replicate to successor task, number of keys replicated: %d", debugNumKeysReplicated));
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    
}
