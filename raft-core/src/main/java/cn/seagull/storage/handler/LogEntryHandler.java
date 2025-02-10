package cn.seagull.storage.handler;

import cn.hutool.core.collection.CollectionUtil;
import cn.seagull.entity.EntryType;
import cn.seagull.entity.LogEntry;
import cn.seagull.entity.LogId;
import cn.seagull.storage.LogManager;
import com.lmax.disruptor.EventHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yupeng
 */
@Slf4j
public class LogEntryHandler implements EventHandler<LogEntryEvent>
{
    private final List<LogEntryEvent> tasks;
    private final int applyBatch;
    private final LogManager logManager;
    private final long timeout;
    private long lastTime;

    public LogEntryHandler(int applyBatch, long timeout, LogManager logManager)
    {
        this.applyBatch = applyBatch;
        this.tasks = new ArrayList<>(applyBatch);
        this.logManager = logManager;
        this.timeout = timeout;
        this.lastTime = System.currentTimeMillis();
    }

    @Override
    public void onEvent(LogEntryEvent event, long sequence, boolean endOfBatch) throws Exception
    {
        this.tasks.add(event);
        boolean isTimeout = (System.currentTimeMillis() - lastTime) / 1000 > timeout;
        if (CollectionUtil.isNotEmpty(tasks) && (this.tasks.size() >= this.applyBatch || isTimeout || endOfBatch)) {
            executeApplyingTasks(this.tasks);
            this.tasks.clear();
        }
    }

    private void executeApplyingTasks(final List<LogEntryEvent> tasks) {
        int size = tasks.size();

        List<LogEntry> entries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            LogEntryEvent task = tasks.get(i);

            LogId logId = new LogId();
            logId.setTerm(task.getCurrTerm());
            task.getEntry().setId(logId);
            task.getEntry().setType(EntryType.DATA);
            entries.add(task.getEntry());
        }

        this.logManager.appendEntries(entries);
    }
}
