package cn.seagull.core;

import cn.seagull.SPI;
import cn.seagull.StateMachine;
import cn.seagull.entity.Configuration;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author yupeng
 */
@SPI
@Slf4j
public class StateMachineAdapter implements StateMachine
{
    @Override
    public void onApply(List<byte[]> data)
    {

    }

    @Override
    public void onConfigurationCommitted(Configuration conf)
    {
        log.debug("applying task to configuration committed {}.", conf);
    }
}
