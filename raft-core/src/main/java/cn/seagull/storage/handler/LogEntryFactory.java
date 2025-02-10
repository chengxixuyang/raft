package cn.seagull.storage.handler;

import com.lmax.disruptor.EventFactory;

/**
 * @author yupeng
 */
public class LogEntryFactory implements EventFactory<LogEntryEvent>
{
    @Override
    public LogEntryEvent newInstance()
    {
        return new LogEntryEvent();
    }
}
