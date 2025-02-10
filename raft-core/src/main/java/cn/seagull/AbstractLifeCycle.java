package cn.seagull;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractLifeCycle implements LifeCycle {

    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    @Override
    public void startup() throws LifeCycleException {
        if (!this.isStarted.compareAndSet(false, true)) {
            throw new LifeCycleException("this component has started");
        }
    }

    @Override
    public void shutdown() throws LifeCycleException {
        if (!this.isStarted.compareAndSet(true, false)) {
            throw new LifeCycleException("this component has closed");
        }
    }

    @Override
    public boolean isStarted() {
        return this.isStarted.get();
    }
}
