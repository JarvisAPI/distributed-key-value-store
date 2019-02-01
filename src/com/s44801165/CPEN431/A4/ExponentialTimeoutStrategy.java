package com.s44801165.CPEN431.A4;

public class ExponentialTimeoutStrategy implements TimeoutStrategy {
    private final int INITIAL_TIMEOUT;
    private int mTimeout;
    
    public ExponentialTimeoutStrategy(int initialTimeout) {
        INITIAL_TIMEOUT = initialTimeout;
        mTimeout = initialTimeout;
    }
    
    @Override
    public int getTimeout() {
        return mTimeout;
    }

    @Override
    public void onTimedOut() {
        mTimeout *= 2;
    }

    @Override
    public void reset() {
        mTimeout = INITIAL_TIMEOUT;
    }

}
