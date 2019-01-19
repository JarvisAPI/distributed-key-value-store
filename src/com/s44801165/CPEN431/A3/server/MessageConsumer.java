package com.s44801165.CPEN431.A3.server;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.BlockingQueue;

import com.google.protobuf.ByteString;
import com.s44801165.CPEN431.A3.protocol.NetworkMessage;
import com.s44801165.CPEN431.A3.protocol.Protocol;
import com.s44801165.CPEN431.A3.server.KeyValueStore.ValuePair;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;

public class MessageConsumer extends Thread {
    private DatagramSocket mSocket;
    private BlockingQueue<NetworkMessage> mQueue;
    private KeyValueStore mKeyValStore;
    private MessageCache mMessageCache;
    
    public MessageConsumer(DatagramSocket socket, BlockingQueue<NetworkMessage> queue) {
        mSocket = socket;
        mQueue = queue;
        mKeyValStore = KeyValueStore.getInstance();
        mMessageCache = MessageCache.getInstance();
    }

    @Override
    public void run() {
        byte[] SUCCESS_BYTES = KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(Protocol.ERR_SUCCESS).build()
                .toByteArray();
        NetworkMessage message;
        ValuePair vPair;
        KeyValueResponse.KVResponse.Builder successResBuilder = KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(Protocol.ERR_SUCCESS);

        byte[] dataBytes;
        int errCode;
        ByteString key;
        ByteString value;
        
        while (true) {
            try {       
                dataBytes = null; 
                errCode = Protocol.ERR_SUCCESS;
                message = mQueue.take();
                
                try {
                    ByteString cachedMessageVal = mMessageCache.get(message.getIdString());
                    if (cachedMessageVal != null) {
                        if (cachedMessageVal != MessageCache.ENTRY_BEING_PROCESSED) {
                            dataBytes = cachedMessageVal.toByteArray();
                            DatagramPacket packet = new DatagramPacket(dataBytes, dataBytes.length,
                                    message.getAddress(), message.getPort());
                            mSocket.send(packet);
                        }
                        // Message is being processed by other thread.
                        continue;
                    } else {
                        try {
                            if (!mMessageCache.putIfNotExist(message.getIdString(),
                                    MessageCache.ENTRY_BEING_PROCESSED)) {
                                // Message is being processed by other thread so move on.
                                continue;
                            }
                        } catch(OutOfMemoryError e) {
                            System.out.println("Out of cache space, signaling overload");
                            message.setPayload(KeyValueResponse.KVResponse.newBuilder()
                                    .setErrCode(Protocol.ERR_SYSTEM_OVERLOAD)
                                    .build()
                                    .toByteArray());
                            dataBytes = message.getDataBytes();
                            mSocket.send(new DatagramPacket(dataBytes, dataBytes.length,
                                    message.getAddress(), message.getPort()));
                            continue;
                        }
                    }
    
                    KeyValueRequest.KVRequest kvReq = KeyValueRequest.KVRequest.newBuilder()
                            .mergeFrom(message.getPayload()).build();
    
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
                                dataBytes = successResBuilder
                                        .setValue(vPair.value)
                                        .setVersion(vPair.version)
                                        .build()
                                        .toByteArray();
                            }   else {
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
                        dataBytes = KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(Protocol.ERR_SUCCESS)
                        .build()
                        .toByteArray();
                        break;
                    case Protocol.IS_ALIVE:
                        dataBytes = KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(Protocol.ERR_SUCCESS)
                        .build()
                        .toByteArray();
                        break;
                    case Protocol.GET_PID:
                        // TODO: get pid somehow.
                        break;
                    case Protocol.GET_MEMBERSHIP_COUNT:
                        dataBytes = KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(Protocol.ERR_SUCCESS)
                        .setMembershipCount(1)
                        .build()
                        .toByteArray();
                        break;
                    default:
                        errCode = Protocol.ERR_UNRECOGNIZED_COMMAND;
                        break;
                    }
                    
                } catch (Exception e) {
                    e.printStackTrace();
                    errCode = Protocol.ERR_INTERNAL_KVSTORE_FAILURE;
                }
                
                if (errCode != Protocol.ERR_SUCCESS) {
                    dataBytes = KeyValueResponse.KVResponse.newBuilder()
                            .setErrCode(errCode)
                            .build()
                            .toByteArray();
                }

                message.setPayload(dataBytes);
                dataBytes = message.getDataBytes();
                
                DatagramPacket packet = new DatagramPacket(dataBytes, dataBytes.length,
                        message.getAddress(), message.getPort());
                
                mMessageCache.put(message.getIdString(), ByteString.copyFrom(dataBytes));
                mSocket.send(packet);

            } catch (Exception e) {
               e.printStackTrace();
            }
        }
    }
}
