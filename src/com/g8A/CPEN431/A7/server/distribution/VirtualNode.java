package com.g8A.CPEN431.A7.server.distribution;

import com.google.protobuf.ByteString;

public class VirtualNode implements Node {

    private final ByteString pNode;
    private int pNodeId;
    private final int vIndex;

    public VirtualNode(ByteString pNode, int pNodeId, int vIndex) {
        this.pNode = pNode;
        this.pNodeId = pNodeId;
        this.vIndex = vIndex;
    }

    public byte[] getKey() {
        return (pNode + "" + vIndex).getBytes();
    }

    public ByteString getPNode() {
        return this.pNode;
    }

    public int getPNodeId() {
        return this.pNodeId;
    }

    public boolean isVirtualNodeOf(ByteString physicalNode) {
        return physicalNode.equals(this.pNode);
    }
}
