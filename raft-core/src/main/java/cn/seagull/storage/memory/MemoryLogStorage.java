package cn.seagull.storage.memory;

import cn.hutool.core.collection.CollectionUtil;
import cn.seagull.AbstractLifeCycle;
import cn.seagull.LifeCycleException;
import cn.seagull.SPI;
import cn.seagull.entity.LogEntry;
import cn.seagull.storage.LogStorage;
import cn.seagull.storage.option.LogStorageOption;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@SPI
@Slf4j
public class MemoryLogStorage extends AbstractLifeCycle implements LogStorage {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock writeLock = this.lock.writeLock();
    private final Lock readLock = this.lock.readLock();
    private volatile long firstLogIndex;
    private volatile long lastLogIndex;
    private Map<Long, LogEntry> logEntryMap;

    @Override
    public void startup() throws LifeCycleException {
        super.startup();

        this.logEntryMap = new HashMap<>(128);
    }

    @Override
    public void init(LogStorageOption option)
    {

    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            return this.firstLogIndex;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        try {
            return this.lastLogIndex;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(long index) {
        this.readLock.lock();
        try {
            return this.logEntryMap.get(index);
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<LogEntry> getEntry(List<Long> logIndexs)
    {
        if (CollectionUtil.isEmpty(logIndexs)) {
            return null;
        }

        this.readLock.lock();
        try {
            return logIndexs.stream().map(logEntryMap::get).filter(Objects::nonNull).collect(Collectors.toList());
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public int appendEntries(List<LogEntry> entries) {
        if (CollectionUtil.isNotEmpty(entries)) {
            this.writeLock.lock();
            try {
                entries.forEach(entry -> {
                    this.logEntryMap.put(entry.getId().getIndex(), entry);
                    log.debug("append log entry {}", entry);
                });
                this.lastLogIndex = entries.getLast().getId().getIndex();
                return entries.size();
            } finally {
                this.writeLock.unlock();
            }
        }

        return 0;
    }

    @Override
    public void setAppliedLogIndex(long appliedLogIndex)
    {

    }

    @Override
    public long getAppliedLogIndex()
    {
        return 0;
    }

    @Override
    public boolean truncatePrefix(long firstIndexKept) {
        this.writeLock.lock();
        try {
            if (firstIndexKept < this.firstLogIndex) {
                return false;
            }

            for (long i = this.firstLogIndex; i <= firstIndexKept; i++)
            {
                this.logEntryMap.remove(i);
            }

            this.firstLogIndex = firstIndexKept;
        } finally {
            this.writeLock.unlock();
        }

        return true;
    }

    @Override
    public boolean truncateSuffix(long lastIndexKept) {
        this.writeLock.lock();
        try {
            if (lastIndexKept > this.lastLogIndex) {
                return false;
            }

            for (long i = lastIndexKept + 1; i <= this.lastLogIndex; i++)
            {
                this.logEntryMap.remove(i);
            }
            this.lastLogIndex = lastIndexKept;
        } finally {
            this.writeLock.unlock();
        }

        return true;
    }
}
