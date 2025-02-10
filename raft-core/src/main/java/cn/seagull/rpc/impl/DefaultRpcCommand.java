package cn.seagull.rpc.impl;

import cn.seagull.rpc.RpcCommand;
import lombok.NoArgsConstructor;

/**
 *
 * @author yupeng
 */
@NoArgsConstructor
public class DefaultRpcCommand implements RpcCommand
{
    private Long invokeId;

    private Object command;

    public DefaultRpcCommand(Long invokeId, Object command)
    {
        this.invokeId = invokeId;
        this.command = command;
    }

    @Override
    public Long invokeId()
    {
        return this.invokeId;
    }

    @Override
    public Object command()
    {
        return this.command;
    }
}
