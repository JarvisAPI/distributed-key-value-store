package com.g8A.CPEN431.A7.server.distribution;

public class VirtualNode implements Node {

    private final int pNode;
    private final long vIndex;

    public VirtualNode(int pNode, long vIndex) {
        this.pNode = pNode;
        this.vIndex = vIndex;
    }

    public byte[] getKey() {
        return (pNode + "" + vIndex).getBytes();
    }

    public int getPNode() {
        return this.pNode;
    }

    public boolean isVirtualNodeOf(int physicalNode) {
        return physicalNode == this.pNode;
    }
}
