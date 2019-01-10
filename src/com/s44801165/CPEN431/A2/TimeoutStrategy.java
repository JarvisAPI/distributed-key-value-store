package com.s44801165.CPEN431.A2;

public interface TimeoutStrategy {
    /**
     * 
     * @return the current timeout amount in milliseconds.
     */
    public int getTimeout();
    
    /**
     * Signal that timeout event has occurred, which allows
     * new timeout to be recalculated.
     */
    public void onTimedOut();
    
    /**
     * Reset the timeout value to the initial value, called when timeout
     * needs to be initialized or reset.
     */
    public void reset();
}
