package cn.seagull.storage.handler;

import cn.seagull.InvokeCallback;
import cn.seagull.entity.LogEntry;
import lombok.Data;

import java.util.concurrent.CountDownLatch;

/**
 * @author yupeng
 */
@Data
public class LogEntryEvent
{
    private LogEntry entry;

    private long currTerm;

    private InvokeCallback callback;
}
