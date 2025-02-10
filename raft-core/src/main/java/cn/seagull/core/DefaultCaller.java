package cn.seagull.core;

import cn.hutool.core.collection.CollectionUtil;
import cn.seagull.AbstractLifeCycle;
import cn.seagull.Caller;
import cn.seagull.StateMachine;
import cn.seagull.entity.Configuration;
import cn.seagull.entity.EntryType;
import cn.seagull.entity.LogEntry;
import cn.seagull.serializer.SerializerFactory;
import cn.seagull.storage.LogManager;
import cn.seagull.util.ServiceLoaderFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author yupeng
 */
@Slf4j
public class DefaultCaller extends AbstractLifeCycle implements Caller
{
    private LogManager logManager;
    private AtomicLong lastAppliedIndex;
    private AtomicLong lastCommitIndex;
    private StateMachine stateMachine;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    public DefaultCaller()
    {
        this.lastAppliedIndex = new AtomicLong(0);
        this.lastCommitIndex = new AtomicLong(0);
    }

    @Override
    public void init(CallerOption option)
    {
        this.stateMachine = ServiceLoaderFactory.loadFirstService(StateMachine.class);
        this.logManager = option.getLogManager();
    }

    @Override
    public boolean commit(long commitIndex)
    {
        this.writeLock.lock();
        try
        {
            long lastAppliedIndex = this.lastCommitIndex.get();
            if (lastAppliedIndex >= commitIndex)
            {
                return false;
            }

            this.lastCommitIndex.set(commitIndex);

            List<byte[]> entries = new ArrayList<>((int) (commitIndex - lastAppliedIndex));
            for (long i = lastAppliedIndex + 1; i <= commitIndex; i++)
            {
                LogEntry entry = this.logManager.getEntry(i);
                if (entry != null)
                {
                    EntryType type = entry.getType();
                    if (type != EntryType.DATA)
                    {
                        if (type == EntryType.CONFIGURATION)
                        {
                            Configuration conf = SerializerFactory.getDefaultSerializer().deserialize(entry.getData());
                            this.stateMachine.onConfigurationCommitted(conf);
                        }
                        continue;
                    }

                    entries.add(entry.getData());
                }
            }

            this.logManager.setAppliedIndex(commitIndex);

            log.debug("commit log index {}", commitIndex);

            if (CollectionUtil.isNotEmpty(entries))
            {
                this.stateMachine.onApply(entries);
            }
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.writeLock.unlock();
        }
        return true;
    }

    @Override
    public long getLastAppliedIndex()
    {
        this.readLock.lock();
        try
        {
            return this.lastAppliedIndex.get();
        }finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastCommittedIndex()
    {
        this.readLock.lock();
        try
        {
            return this.lastCommitIndex.get();
        }finally {
            this.readLock.unlock();
        }
    }
}
