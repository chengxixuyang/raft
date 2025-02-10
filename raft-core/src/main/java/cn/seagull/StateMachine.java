package cn.seagull;

import cn.seagull.entity.Configuration;
import cn.seagull.entity.LogEntry;

import java.util.Iterator;
import java.util.List;

/**
 * @author yupeng
 */
public interface StateMachine
{
    void onApply(List<byte[]> data);

    void onConfigurationCommitted(Configuration conf);
}
