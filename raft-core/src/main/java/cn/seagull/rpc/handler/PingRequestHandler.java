package cn.seagull.rpc.handler;

import cn.seagull.Node;
import cn.seagull.NodeManager;
import cn.seagull.SPI;
import cn.seagull.entity.PingRequest;

/**
 * @author yupeng
 */
@SPI
public class PingRequestHandler implements RpcHandler
{
    @Override
    public boolean isAssignableFrom(Class<?> cls)
    {
        return PingRequest.class.isAssignableFrom(cls);
    }

    @Override
    public Object process(Object msg)
    {
        PingRequest pingRequest = (PingRequest) msg;
        Node node = NodeManager.getInstance().get(pingRequest.getTargetId().getGroupId());
        if (node != null) {
            node.resetLastLeaderTimestamp(System.currentTimeMillis());
        }

        return null;
    }
}
