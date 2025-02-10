package cn.seagull.storage;

import cn.seagull.LifeCycle;
import cn.seagull.entity.LogEntry;
import cn.seagull.storage.option.LogStorageOption;

import java.util.List;

public interface LogStorage extends LifeCycle {

    void init(LogStorageOption option);

    /**
     * Returns first log index in log.
     */
    long getFirstLogIndex();

    /**
     * Returns last log index in log.
     */
    long getLastLogIndex();

    /**
     * Get logEntry by index.
     */
    LogEntry getEntry(long index);

    List<LogEntry> getEntry(List<Long> logIndexs);

    /**
     * Append entries to log, return append success number.
     */
    int appendEntries(List<LogEntry> entries);

    void setAppliedLogIndex(long appliedLogIndex);

    long getAppliedLogIndex();

    /**
     * Delete logs from storage's head, [first_log_index, first_index_kept) will
     * be discarded.
     */
    boolean truncatePrefix(long firstIndexKept);

    /**
     * Delete uncommitted logs from storage's tail, (last_index_kept, last_log_index]
     * will be discarded.
     */
    boolean truncateSuffix(long lastIndexKept);
}
