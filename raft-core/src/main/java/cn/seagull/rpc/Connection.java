package cn.seagull.rpc;

import cn.seagull.entity.NodeId;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;

@Data
public class Connection
{
    private Channel channel;
    private NodeId nodeId;
    private final ConcurrentHashMap<Long, InvokeFuture> invokeFutureMap = new ConcurrentHashMap<>(8);
    public static final AttributeKey<Connection> CONNECTION = AttributeKey.valueOf("connection");

    public Connection(Channel channel)
    {
        this.channel = channel;
        this.channel.attr(CONNECTION).set(this);
    }

    public Connection(Channel channel, NodeId nodeId)
    {
        this(channel);
        this.nodeId = nodeId;
    }

    public InvokeFuture getInvokeFuture(long id) {
        return this.invokeFutureMap.get(id);
    }


    public InvokeFuture addInvokeFuture(InvokeFuture future) {
        return this.invokeFutureMap.putIfAbsent(future.getInvokeId(), future);
    }

    public InvokeFuture removeInvokeFuture(long id) {
        return this.invokeFutureMap.remove(id);
    }
}
