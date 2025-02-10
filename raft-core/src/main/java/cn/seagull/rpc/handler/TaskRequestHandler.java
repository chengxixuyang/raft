package cn.seagull.rpc.handler;

import cn.seagull.Node;
import cn.seagull.NodeManager;
import cn.seagull.SPI;
import cn.seagull.core.Task;

/**
 * @author yupeng
 */
@SPI
public class TaskRequestHandler implements RpcHandler
{
    @Override
    public boolean isAssignableFrom(Class<?> cls)
    {
        return Task.class.isAssignableFrom(cls);
    }

    @Override
    public Object process(Object msg)
    {
        Task task = (Task) msg;
        Node leader = NodeManager.getInstance().get(task.getGroupId());
        if (leader != null) {
            leader.apply(task);
        }

        return null;
    }
}
