package cn.seagull.rpc;

import cn.seagull.InvokeCallback;
import cn.seagull.LifeCycle;
import cn.seagull.entity.NodeId;
import cn.seagull.rpc.option.RpcOption;

public interface RpcClient extends LifeCycle {

    void init(RpcOption option);

    boolean createConnection(NodeId nodeId);

    Object invokeSync(NodeId nodeId, Object request) throws InterruptedException;

    Object invokeSync(NodeId nodeId, Object request, int timeoutMillis) throws InterruptedException;

    void invokeWithCallback(NodeId nodeId, Object request, InvokeCallback invokeCallback);

    void invokeWithCallback(NodeId nodeId, Object request, InvokeCallback invokeCallback, int timeoutMillis);

    void invokeAsync(NodeId nodeId, Object request);
}
