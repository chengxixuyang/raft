package cn.seagull.rpc;

/**
 * @author yupeng
 */
public interface RpcCommand
{
    Long invokeId();

    Object command();
}
