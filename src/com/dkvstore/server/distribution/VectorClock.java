package com.dkvstore.server.distribution;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;

public final class VectorClock {
    public static enum CompareResult {
        Larger,
        Smaller,
        Equal,
        Uncomparable
    }
    /**
     * Compares two vector clocks.
     * @param vClockA a vector clock clock.
     * @param vClockB another vector clock.
     * @return compare result that indicates that clockA is "larger,smaller,etc." than clockB.
     */
    public static CompareResult compareVectorClock(int[] vClockA, int[] vClockB) {
        int curNodeId;

        int numLargerEntriesInA = 0;
        int numLargerEntriesInB = 0;
        
        for(int i = 0; i < vClockA.length; i+=2) {
            curNodeId = vClockA[i];
            int clockASeqNum = vClockA[i+1];
            int clockBSeqNum = getSequenceNumber(vClockB, curNodeId);
            
            if (clockASeqNum > clockBSeqNum) {
                numLargerEntriesInA++;
            } else if (clockASeqNum < clockBSeqNum) {
                numLargerEntriesInB++;
            }
        }
        
        if (numLargerEntriesInA == 0 && numLargerEntriesInB == 0) {
            return CompareResult.Equal;
        }
        if (numLargerEntriesInA > 0 && numLargerEntriesInB == 0) {
            return CompareResult.Larger;
        }
        if (numLargerEntriesInA == 0 && numLargerEntriesInB > 0) {
            return CompareResult.Smaller;
        }
        return CompareResult.Uncomparable;
    }
    
    /**
     * Get the sequence number associated with given node id in the given vector clock.
     * @param vectorClock the given vector clock.
     * @param nodeId the node id to match.
     * @return the sequence number found in the vector clock or 0 if no entry is found.
     */
    private static int getSequenceNumber(int[] vectorClock, int nodeId) {
        for (int i = 0; i < vectorClock.length; i+=2) {
            if (vectorClock[i] == nodeId) {
                return vectorClock[i+1];
            }
        }
        return 0;
    }
    
    /**
     * Increments an entry in the vector clock.
     * @param vectorClock the vector clock.
     * @param kvNodeId the node id entry to increment.
     * @return the incremented vector clock.
     */
    public static int[] incrementVectorClock(int[] vectorClock, int kvNodeId) {
        boolean vectorClockContainsNodeId = false;
        
        for(int i = 0; i < vectorClock.length; i+=2) {
            if(vectorClock[i] == kvNodeId) {
                vectorClockContainsNodeId = true;
                vectorClock[i+1]++;
            }
        }
        
        if(!vectorClockContainsNodeId) {
            int[] newVectorClock = new int[vectorClock.length + 2];
            for(int i = 0; i < vectorClock.length; i++) {
                newVectorClock[i] = vectorClock[i];
            }
            newVectorClock[vectorClock.length] = kvNodeId;
            newVectorClock[vectorClock.length+1] = 1;
            
            vectorClock = newVectorClock;
        }
        return vectorClock;
    }
    
    /**
     * Create a new vector clock with a single entry (nodeId, seqNum).
     * @param nodeId the nodeId to put in the vector clock.
     * @param seqNum the sequence number to associate with nodeId.
     * @return the newly created vector clock.
     */
    public static int[] create(int nodeId, int seqNum) {
        int[] newVClock = new int[2];
        newVClock[0] = nodeId;
        newVClock[1] = seqNum;
        return newVClock;
    }
    
    /**
     * Get the vector clock associated with a request.
     * @param kvReqBuilder the builder assocaited with the request.
     * @return a newly created vector clock or null if no clock exists in the builder.
     */
    public static int[] getVectorClock(KeyValueRequest.KVRequest.Builder kvReqBuilder) {
        int numInts = kvReqBuilder.getVectorClockCount();
        
        if (numInts > 0) {
            int[] vectorClock = new int[numInts];
            for (int i = 0; i < numInts; i++) {
                vectorClock[i] = kvReqBuilder.getVectorClock(i);
            }
            return vectorClock;
        }
        return null;
    }
}
