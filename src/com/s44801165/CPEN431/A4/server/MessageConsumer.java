package com.s44801165.CPEN431.A4.server;

import java.lang.management.ManagementFactory;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.BlockingQueue;

import com.google.protobuf.ByteString;
import com.s44801165.CPEN431.A4.protocol.NetworkMessage;
import com.s44801165.CPEN431.A4.protocol.Protocol;
import com.s44801165.CPEN431.A4.server.KeyValueStore.ValuePair;
import com.s44801165.CPEN431.A4.server.MessageCache.CacheEntry;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;

public class MessageConsumer extends Thread {
    private DatagramSocket mSocket;
    private BlockingQueue<NetworkMessage> mQueue;
    private KeyValueStore mKeyValStore;
    private MessageCache mMessageCache;
    
    private static final int CACHE_META_COMPLETE_RESPONSE = 0;
    private static final int CACHE_META_SUCCESS_BYTES = 1;
    private static final int CACHE_META_SUCCESS_GET = 2;
    
    public MessageConsumer(DatagramSocket socket, BlockingQueue<NetworkMessage> queue) {
        mSocket = socket;
        mQueue = queue;
        mKeyValStore = KeyValueStore.getInstance();
        mMessageCache = MessageCache.getInstance();
    }

    @Override
    public void run() {
        KeyValueResponse.KVResponse.Builder kvResBuilder = KeyValueResponse.KVResponse.newBuilder();
        byte[] SUCCESS_BYTES = kvResBuilder
                .setErrCode(Protocol.ERR_SUCCESS)
                .build()
                .toByteArray();

        NetworkMessage message;
        ValuePair vPair;

        byte[] dataBytes;
        int errCode;
        ByteString key;
        ByteString value;
        int cacheMetaInfo;
        int metaInfo;
        
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
                    } else {
                        try {
                            if (!mMessageCache.putIfNotExist(message.getIdString(),
                                    MessageCache.ENTRY_BEING_PROCESSED, CACHE_META_COMPLETE_RESPONSE, 0)) {
                                // Message is being processed by other thread so move on.
                                continue;
                            }
                        } catch (OutOfMemoryError e) {
                            message.setPayload(kvResBuilder
                                    .setErrCode(Protocol.ERR_SYSTEM_OVERLOAD)
                                    .setOverloadWaitTime(Protocol.OVERLOAD_WAITTIME)
                                    .build()
                                    .toByteArray());
                            dataBytes = message.getDataBytes();
                            packet.setData(dataBytes);
                            packet.setAddress(message.getAddress());
                            packet.setPort(message.getPort());
                            mSocket.send(packet);
                            continue;
                        }
                    }
    
                    KeyValueRequest.KVRequest kvReq = KeyValueRequest.KVRequest
                            .parseFrom(message.getPayload());
    
                    key = kvReq.getKey();
                    value = kvReq.getValue();
    
                    switch (kvReq.getCommand()) {
                    case Protocol.PUT: {
                        if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                            errCode = Protocol.ERR_INVALID_KEY;
                        } else if (value.size() > Protocol.SIZE_MAX_VAL_LENGTH) {
                            errCode = Protocol.ERR_INVALID_VAL;
                        } else {
                            mKeyValStore.put(key, value, kvReq.getVersion());
                            dataBytes = SUCCESS_BYTES;
                            cacheMetaInfo = CACHE_META_SUCCESS_BYTES | MessageCache.META_MASK_CACHE_REFERENCE;
                        }
                        break;
                    }
                    case Protocol.GET: {
                        if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                            errCode = Protocol.ERR_INVALID_KEY;
                        } else if (!value.isEmpty()) {
                            errCode = Protocol.ERR_INVALID_VAL;
                        } else {
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
                        break;
                    }
                    case Protocol.REMOVE: {
                        if (key.isEmpty() || key.size() > Protocol.SIZE_MAX_KEY_LENGTH) {
                            errCode = Protocol.ERR_INVALID_KEY;
                        } else if (!value.isEmpty()) {
                            errCode = Protocol.ERR_INVALID_VAL;
                        } else if (mKeyValStore.remove(key)) {
                            dataBytes = SUCCESS_BYTES;
                            cacheMetaInfo = CACHE_META_SUCCESS_BYTES | MessageCache.META_MASK_CACHE_REFERENCE;
                        } else {
                            errCode = Protocol.ERR_NON_EXISTENT_KEY;
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

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
