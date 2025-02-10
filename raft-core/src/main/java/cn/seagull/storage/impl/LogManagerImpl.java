package cn.seagull.storage.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.seagull.AbstractLifeCycle;
import cn.seagull.LifeCycleException;
import cn.seagull.entity.LogEntry;
import cn.seagull.entity.LogId;
import cn.seagull.storage.LogManager;
import cn.seagull.storage.LogStorage;
import cn.seagull.storage.option.LogManagerOption;
import cn.seagull.storage.option.LogStorageOption;
import cn.seagull.util.ServiceLoaderFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class LogManagerImpl extends AbstractLifeCycle implements LogManager {
    private LogStorage logStorage;
    private volatile long firstLogIndex;
    private volatile long lastLogIndex;
    private volatile long appliedIndex;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    @Override
    public void init(LogManagerOption option) {
        this.logStorage = ServiceLoaderFactory.loadFirstService(LogStorage.class);
        LogStorageOption storageOption = new LogStorageOption();
        storageOption.setPath(option.getPath());
        storageOption.setSync(option.isSync());
        storageOption.setConf(option.getConf());
        this.logStorage.init(storageOption);
        this.firstLogIndex = this.logStorage.getFirstLogIndex();
        this.lastLogIndex = this.logStorage.getLastLogIndex();
        this.appliedIndex = this.logStorage.getAppliedLogIndex();
    }

    @Override
    public void startup() throws LifeCycleException {
        super.startup();

        this.logStorage.startup();
    }

    @Override
    public void appendEntries(List<LogEntry> entries) {
        writeLock.lock();
        try
        {
            if (CollectionUtil.isNotEmpty(entries) && !checkAndResolveConflict(entries))
            {
                entries.clear();
                return;
            }

            this.logStorage.appendEntries(entries);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(long index) {
        return this.logStorage.getEntry(index);
    }

    @Override
    public List<LogEntry> getEntry(List<Long> logIndexs)
    {
        return this.logStorage.getEntry(logIndexs);
    }

    @Override
    public long getTerm(long index) {
        LogEntry logEntry = this.logStorage.getEntry(index);
        if (logEntry != null) {
            return logEntry.getId().getTerm();
        }

        return -1;
    }

    @Override
    public long getFirstLogIndex() {
        return this.firstLogIndex;
    }

    @Override
    public long getLastLogIndex() {
        return this.lastLogIndex;
    }

    @Override
    public LogId getLastLogId() {
        readLock.lock();
        try
        {

            long index = lastLogIndex;
            if (index >= firstLogIndex)
            {
                return new LogId(index, getTerm(index));
            }

            return null;
        }
        finally
        {
            readLock.unlock();
        }
    }

    @Override
    public void setAppliedIndex(long appliedIndex)
    {
       if (appliedIndex == this.appliedIndex) {
           return;
       }

       this.logStorage.setAppliedLogIndex(appliedIndex);
       this.appliedIndex= appliedIndex;
    }

    private boolean checkAndResolveConflict(List<LogEntry> entries) {
        LogEntry firstLogEntry = entries.getFirst();
        if (firstLogEntry.getId().getIndex() == 0) {
            for (int i = 0; i < entries.size(); i++) {
                entries.get(i).getId().setIndex(++this.lastLogIndex);
            }
            return true;
        } else {
            long lastLogIndex = this.lastLogIndex;
            if (firstLogEntry.getId().getIndex() > lastLogIndex + 1) {
                return false;
            }

            long appliedIndex = this.appliedIndex;
            LogEntry lastLogEntry = entries.getLast();
            if (lastLogEntry.getId().getIndex() <= appliedIndex) {
                log.warn(
                        "Received entries of which the lastLog={} is not greater than appliedIndex={}, return immediately with nothing changed.",
                        lastLogEntry.getId().getIndex(), appliedIndex);
                return false;
            }

            if (firstLogEntry.getId().getIndex() == lastLogIndex + 1) {
                this.lastLogIndex = lastLogEntry.getId().getIndex();
            } else {
                int conflictingIndex = 0;
                for (; conflictingIndex < entries.size(); conflictingIndex++) {
                    if (getTerm(entries.get(conflictingIndex).getId().getIndex()) != entries.get(conflictingIndex).getId().getTerm()) {
                        break;
                    }
                }

                if (conflictingIndex != entries.size()) {
                    if (entries.get(conflictingIndex).getId().getIndex() <= lastLogIndex) {
                        this.logStorage.truncateSuffix(entries.get(conflictingIndex).getId().getIndex() - 1);
                    }
                    this.lastLogIndex = lastLogEntry.getId().getIndex();
                }

                if (conflictingIndex > 0) {
                    entries.subList(0, conflictingIndex).clear();
                }
            }

            return true;
        }
    }
}
