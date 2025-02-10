package cn.seagull.rpc.handler;

import cn.seagull.Node;
import cn.seagull.NodeManager;
import cn.seagull.SPI;
import cn.seagull.entity.AddNode;

/**
 * @author yupeng
 */
@SPI
public class AddNodeHandler implements RpcHandler
{
    @Override
    public boolean isAssignableFrom(Class<?> cls)
    {
        return AddNode.class.isAssignableFrom(cls);
    }

    @Override
    public Object process(Object msg)
    {
        AddNode node = (AddNode) msg;

        Node leader = NodeManager.getInstance().get(node.getEndpoint().getGroupId());
        if (leader != null)
        {
            leader.addNode(node.getEndpoint());
        }

        return null;
    }
}
