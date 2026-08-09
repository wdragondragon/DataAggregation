package com.jdragon.aggregation.transfer;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TransferControl {

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition resumed = lock.newCondition();
    private volatile boolean pauseRequested;
    private volatile boolean canceled;

    public void pause() {
        pauseRequested = true;
    }

    public void resume() {
        lock.lock();
        try {
            pauseRequested = false;
            resumed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void cancel() {
        lock.lock();
        try {
            canceled = true;
            pauseRequested = false;
            resumed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean isPauseRequested() {
        return pauseRequested;
    }

    public boolean isCanceled() {
        return canceled;
    }

    public void awaitIfPaused() throws InterruptedException, TransferCanceledException {
        if (canceled) {
            throw new TransferCanceledException();
        }
        if (!pauseRequested) {
            return;
        }
        lock.lockInterruptibly();
        try {
            while (pauseRequested && !canceled) {
                resumed.await();
            }
            if (canceled) {
                throw new TransferCanceledException();
            }
        } finally {
            lock.unlock();
        }
    }
}
