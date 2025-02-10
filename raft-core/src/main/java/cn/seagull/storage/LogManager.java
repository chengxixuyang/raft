package cn.seagull.storage;

import cn.seagull.LifeCycle;
import cn.seagull.entity.LogEntry;
import cn.seagull.entity.LogId;
import cn.seagull.storage.option.LogManagerOption;

import java.util.List;

public interface LogManager extends LifeCycle {

    void init(LogManagerOption option);

    void appendEntries(List<LogEntry> entries);

    LogEntry getEntry(long index);

    List<LogEntry> getEntry(List<Long> logIndexs);

    long getTerm(long index);

    long getFirstLogIndex();

    long getLastLogIndex();

    LogId getLastLogId();

    void setAppliedIndex(long appliedIndex);
}
