package cn.seagull.rpc.handler;

import cn.seagull.Node;
import cn.seagull.NodeManager;
import cn.seagull.SPI;
import cn.seagull.entity.RemoveNode;

/**
 * @author yupeng
 */
@SPI
public class RemoveNodeHandler implements RpcHandler
{
    @Override
    public boolean isAssignableFrom(Class<?> cls)
    {
        return RemoveNode.class.isAssignableFrom(cls);
    }

    @Override
    public Object process(Object msg)
    {
        RemoveNode node = (RemoveNode) msg;

        Node leader = NodeManager.getInstance().get(node.getEndpoint().getGroupId());
        if (leader != null)
        {
            leader.removeNode(node.getEndpoint());
        }

        return null;
    }
}
