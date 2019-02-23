package com.g8A.CPEN431.A7.server;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import com.g8A.CPEN431.A7.client.KVClient;
import com.g8A.CPEN431.A7.protocol.NetworkMessage;
import com.g8A.CPEN431.A7.protocol.Protocol;
import com.g8A.CPEN431.A7.server.MessageCache.CacheEntry;
import com.g8A.CPEN431.A7.server.distribution.DirectRoute;
import com.g8A.CPEN431.A7.server.distribution.HashEntity;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy;
import com.g8A.CPEN431.A7.server.distribution.RouteStrategy.AddressHolder;
import com.google.protobuf.ByteString;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;

public class MessageConsumer extends Thread {
    private DatagramSocket mSocket;
    private NetworkQueue mQueue;
    private KeyValueStore mKeyValStore;
    private MessageCache mMessageCache;
    private HashEntity mHashEntity;
    private RouteStrategy mRouteStrat;
    private KVClient mKVClient;
    private int mNodeId;
    private static boolean mIsMigrating;
    private static List<long[]> mAffectedRanges;
    
    private static final int CACHE_META_COMPLETE_RESPONSE = 0;
    private static final int CACHE_META_SUCCESS_BYTES = 1;
    private static final int CACHE_META_SUCCESS_GET = 2;
    
    private KeyValueRequest.KVRequest.Builder kvReqBuilder = KeyValueRequest.KVRequest.newBuilder();

    public MessageConsumer(DatagramSocket socket, NetworkQueue queue, KVClient kvClient, int nodeId) {
        mSocket = socket;
        mQueue = queue;
        mKeyValStore = KeyValueStore.getInstance();
        mMessageCache = MessageCache.getInstance();
        mHashEntity = HashEntity.getInstance();
        mRouteStrat = DirectRoute.getInstance();
        mKVClient = kvClient;
        mNodeId = nodeId;
        mIsMigrating = false;
        mAffectedRanges = null;
    }

    @Override
    public void run() {
        KeyValueResponse.KVResponse.Builder kvResBuilder = KeyValueResponse.KVResponse.newBuilder();
        byte[] SUCCESS_BYTES = kvResBuilder
                .setErrCode(Protocol.ERR_SUCCESS)
                .build()
                .toByteArray();

        NetworkMessage message;
        KeyValueStore.ValuePair vPair;
        byte[] dataBytes;
        int errCode;
        ByteString key;
        ByteString value;
        int cacheMetaInfo;
        int metaInfo;
        int kvNodeId;
        
        DatagramPacket packet = new DatagramPacket(new byte[0], 0, null, 0);
        
        while (true) {
            try {       
                dataBytes = null; 
                errCode = Protocol.ERR_SUCCESS;
                message = mQueue.take();               
                kvResBuilder.clear();
                cacheMetaInfo = 0;
                
                try {
                    CacheEntry entry = mMessageCache.get(message.getIdString());
                    if (entry != null) {
                        ByteString cachedMessageVal = entry.value;
                        if (cachedMessageVal == MessageCache.ENTRY_BEING_PROCESSED) {
                            // Message is being processed by other thread.
                            continue;
                        }
                        metaInfo = (entry.metaInfo & 0x0000ffff);
                        switch(metaInfo) {
                        case CACHE_META_SUCCESS_GET: {
                            dataBytes = kvResBuilder
                                    .setErrCode(Protocol.ERR_SUCCESS)
                                    .setValue(cachedMessageVal)
                                    .setVersion(entry.intField0)
                                    .build()
                                    .toByteArray();
                            message.setPayload(dataBytes);
                            dataBytes = message.getDataBytes();
                            break;
                        }
                        case CACHE_META_SUCCESS_BYTES: {
                            message.setPayload(SUCCESS_BYTES);
                            dataBytes = message.getDataBytes();
                            break;
                        }
                        default:
                            dataBytes = cachedMessageVal.toByteArray();
                        }
                        packet.setData(dataBytes);
                        packet.setAddress(message.getAddress());
                        packet.setPort(message.getPort());
                        mSocket.send(packet);
                        continue;
                    } 
                    else {
                        try {
                            if (!mMessageCache.putIfNotExist(message.getIdString(),
                                    MessageCache.ENTRY_BEING_PROCESSED, CACHE_META_COMPLETE_RESPONSE, 0)) {
                                // Message is being processed by other thread so move on.
                                continue;
                            }
                        } catch (OutOfMemoryError e) {
                            sendOverloadMessage(message, kvResBuilder, packet);
                            continue;
                        }
                    }
    
                    kvReqBuilder.clear();
                    kvReqBuilder = kvReqBuilder.mergeFrom(message.getPayload());
    
                    key = kvReqBuilder.getKey();
                    value = kvReqBuilder.getValue();
                    kvNodeId = HashEntity.getInstance().getKVNodeId(key);
                    
                    switch (kvReqBuilder.getCommand()) {
                    case Protocol.PUT: {
                        if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                            errCode = Protocol.ERR_INVALID_KEY;
                        } else if (value.size() > Protocol.SIZE_MAX_VAL_LENGTH) {
                            errCode = Protocol.ERR_INVALID_VAL;
                        } else if (mIsMigrating && MembershipService.isKeyAffected(key, mAffectedRanges)){
                        	sendOverloadMessage(message, kvResBuilder, packet);
                        	// message overload because of migration, move on
                        	continue;
                        } else {
                            int nodeId = mHashEntity.getKVNodeId(key);
                            if(nodeId != mNodeId) {
                                routeToNode(message, nodeId);
                                // message being processed by other node, move on
                                continue;
                            }
                            else {
                                mKeyValStore.put(key, value, kvReqBuilder.getVersion());
                                dataBytes = SUCCESS_BYTES;
                                cacheMetaInfo = CACHE_META_SUCCESS_BYTES | MessageCache.META_MASK_CACHE_REFERENCE;
                            }
                        }
                        break;
                    }
                    case Protocol.GET: {
                        if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                            errCode = Protocol.ERR_INVALID_KEY;
                        } else if (!value.isEmpty()) {
                            errCode = Protocol.ERR_INVALID_VAL;
                        } else if (mIsMigrating && MembershipService.isKeyAffected(key, mAffectedRanges)){
                        	sendOverloadMessage(message, kvResBuilder, packet);
                        	// message overload because of migration, move on
                        	continue;
                        } else {
                            int nodeId = mHashEntity.getKVNodeId(key);
                            if(nodeId != mNodeId) {
                                routeToNode(message, nodeId);
                                // message being processed by other node, move on
                                continue;
                            }
                            else {
                                vPair = mKeyValStore.get(key);
                                if (vPair != null) {
                                    dataBytes = kvResBuilder
                                            .setErrCode(Protocol.ERR_SUCCESS)
                                            .setValue(vPair.value)
                                            .setVersion(vPair.version)
                                            .build()
                                            .toByteArray();
                                    cacheMetaInfo = CACHE_META_SUCCESS_GET | MessageCache.META_MASK_CACHE_REFERENCE;
                                } else {
                                    errCode = Protocol.ERR_NON_EXISTENT_KEY;
                                }
                            }
                        }
                        break;
                    }
                    case Protocol.REMOVE: {
                        if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                            errCode = Protocol.ERR_INVALID_KEY;
                        } else if (!value.isEmpty()) {
                            errCode = Protocol.ERR_INVALID_VAL;
                        } else {
                            int nodeId = mHashEntity.getKVNodeId(key);
                            if(nodeId != mNodeId) {
                                routeToNode(message, nodeId);
                                // message being processed by other node, move on
                                continue;
                            }
                            else {
                                if (mKeyValStore.remove(key)) {
                                    dataBytes = SUCCESS_BYTES;
                                    cacheMetaInfo = CACHE_META_SUCCESS_BYTES | MessageCache.META_MASK_CACHE_REFERENCE;
                                } else {
                                    errCode = Protocol.ERR_NON_EXISTENT_KEY;
                                }
                            }
                        }
                        break;
                    }
                    case Protocol.SHUTDOWN:
                        System.exit(0);
                        break;
                    case Protocol.WIPEOUT:
                        mKeyValStore.removeAll();                        
                        dataBytes = SUCCESS_BYTES;
                        cacheMetaInfo = CACHE_META_SUCCESS_BYTES | MessageCache.META_MASK_CACHE_REFERENCE;
                        break;
                    case Protocol.IS_ALIVE:
                        dataBytes = SUCCESS_BYTES;
                        cacheMetaInfo = CACHE_META_SUCCESS_BYTES | MessageCache.META_MASK_CACHE_REFERENCE;
                        break;
                    case Protocol.GET_PID: {
                        String vmName = ManagementFactory.getRuntimeMXBean().getName();
                        int p = vmName.indexOf("@");
                        int pid = Integer.valueOf(vmName.substring(0, p));
                        dataBytes = kvResBuilder
                                .setErrCode(Protocol.ERR_SUCCESS)
                                .setPid(pid)
                                .build()
                                .toByteArray();
                        break;
                    }
                    case Protocol.GET_MEMBERSHIP_COUNT:
                        dataBytes = kvResBuilder
                        .setErrCode(Protocol.ERR_SUCCESS)
                        .setMembershipCount(1)
                        .build()
                        .toByteArray();
                        break;
                    default:
                        errCode = Protocol.ERR_UNRECOGNIZED_COMMAND;
                        break;
                    }
                    
                } catch (IllegalStateException e) {
                    sendOverloadMessage(message, kvResBuilder, packet);
                    continue;
                } catch (OutOfMemoryError e) {
                    errCode = Protocol.ERR_OUT_OF_SPACE;
                } catch (Exception e) {
                    e.printStackTrace();
                    errCode = Protocol.ERR_INTERNAL_KVSTORE_FAILURE;
                }
                
                if (errCode != Protocol.ERR_SUCCESS) {
                    dataBytes = kvResBuilder
                            .setErrCode(errCode)
                            .build()
                            .toByteArray();
                }

                message.setPayload(dataBytes);
                dataBytes = message.getDataBytes();
                
                packet.setData(dataBytes);
                packet.setAddress(message.getAddress());
                packet.setPort(message.getPort());
                
                metaInfo = cacheMetaInfo & 0x0000ffff;
                switch(metaInfo) {
                case CACHE_META_SUCCESS_GET:
                    mMessageCache.put(message.getIdString(), kvResBuilder.getValue(), cacheMetaInfo, kvResBuilder.getVersion());
                    break;
                case CACHE_META_SUCCESS_BYTES:
                    mMessageCache.put(message.getIdString(), null, cacheMetaInfo, 0);
                    break;
                default:
                    mMessageCache.put(message.getIdString(), ByteString.copyFrom(dataBytes), cacheMetaInfo, 0);
                }
                
                mSocket.send(packet);
                
                if (kvReqBuilder.hasReplyIpAddress() && kvReqBuilder.hasReplyPort()) {
                    packet.setAddress(InetAddress.getByName(kvReqBuilder.getReplyIpAddress()));
                    packet.setPort(kvReqBuilder.getReplyPort());
                    mSocket.send(packet);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    private void routeToNode(NetworkMessage message, int nodeId) throws UnknownHostException {
        kvReqBuilder.setReplyIpAddress(message.getAddress().getHostAddress());
        kvReqBuilder.setReplyPort(message.getPort());
        message.setPayload(kvReqBuilder.build().toByteArray());
        AddressHolder fromAddress = new AddressHolder(message.getAddress(), message.getPort());
        
        AddressHolder routedNode = mRouteStrat.getRoute(nodeId);
        message.setAddressAndPort(routedNode.address, routedNode.port);
        mKVClient.send(message, fromAddress);
    }
    
    private void sendOverloadMessage(NetworkMessage message, KVResponse.Builder kvResBuilder, DatagramPacket packet) throws IOException {
        message.setPayload(kvResBuilder
                .setErrCode(Protocol.ERR_SYSTEM_OVERLOAD)
                .setOverloadWaitTime(Protocol.getOverloadWaittime())
                .build()
                .toByteArray());
        byte[] dataBytes = message.getDataBytes();
        packet.setData(dataBytes);
        packet.setAddress(message.getAddress());
        packet.setPort(message.getPort());
        mSocket.send(packet);
    }
    
    public static synchronized void startMigration(List<long[]> affectedRanges) {
    	mIsMigrating = true;
    	mAffectedRanges = affectedRanges;
    }
    
    public static synchronized void stopMigration() {
    	mIsMigrating = false;
    	mAffectedRanges = null;
    }
}
