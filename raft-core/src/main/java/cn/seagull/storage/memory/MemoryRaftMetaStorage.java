package cn.seagull.storage.memory;

import cn.seagull.AbstractLifeCycle;
import cn.seagull.SPI;
import cn.seagull.entity.Endpoint;
import cn.seagull.entity.NodeId;
import cn.seagull.storage.RaftMetaStorage;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@SPI
public class MemoryRaftMetaStorage extends AbstractLifeCycle implements RaftMetaStorage {

    private Endpoint voteFor;

    private long term;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final Lock writeLock = this.readWriteLock.writeLock();

    private final Lock readLock = this.readWriteLock.readLock();

    @Override
    public boolean setVotedFor(Endpoint endpoint) {
        writeLock.lock();
        try {
            if (isStarted()) {
                this.voteFor = endpoint;
                return true;
            }
        }finally {
            writeLock.unlock();
        }

        return false;
    }

    @Override
    public boolean setTerm(long term)
    {
        writeLock.lock();
        try {
            if (isStarted()) {
                this.term = term;
                return true;
            }
        }finally {
            writeLock.unlock();
        }

        return false;
    }

    @Override
    public long getTerm()
    {
        readLock.lock();
        try {
            return this.term;
        }finally {
            readLock.unlock();
        }
    }

    @Override
    public Endpoint getVotedFor() {
        readLock.lock();
        try {
            return this.voteFor;
        }finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean setTermAndVotedFor(long term, Endpoint endpoint)
    {
        writeLock.lock();
        try {
            if (isStarted()) {
                this.term = term;
                this.voteFor = endpoint;
                return true;
            }
        }finally {
            writeLock.unlock();
        }
        return false;
    }
}
