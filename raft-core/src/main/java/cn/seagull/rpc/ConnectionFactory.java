package cn.seagull.rpc;

import cn.seagull.LifeCycle;
import cn.seagull.entity.NodeId;
import cn.seagull.rpc.option.ConnectionOption;
import io.netty.channel.ChannelFuture;

public interface ConnectionFactory extends LifeCycle {

    void init(ConnectionOption option);

    ChannelFuture createConnection(NodeId nodeId) throws Exception;

    ChannelFuture createConnection(String targetIp, int targetPort, int connectTimeout);
}
