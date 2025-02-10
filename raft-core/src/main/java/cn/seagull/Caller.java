package cn.seagull;

import cn.seagull.core.CallerOption;

/**
 * @author yupeng
 */
public interface Caller extends LifeCycle
{
    void init(CallerOption option);

    boolean commit(long commitIndex);

    long getLastAppliedIndex();

    long getLastCommittedIndex();
}
