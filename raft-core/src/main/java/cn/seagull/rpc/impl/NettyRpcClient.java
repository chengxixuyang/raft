package cn.seagull.rpc.impl;

import cn.hutool.core.lang.Snowflake;
import cn.hutool.core.util.IdUtil;
import cn.seagull.AbstractLifeCycle;
import cn.seagull.InvokeCallback;
import cn.seagull.LifeCycleException;
import cn.seagull.SPI;
import cn.seagull.entity.NodeId;
import cn.seagull.rpc.*;
import cn.seagull.rpc.option.ConnectionOption;
import cn.seagull.rpc.option.RpcOption;
import io.netty.channel.*;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@SPI
@Slf4j
public class NettyRpcClient extends AbstractLifeCycle implements RpcClient {

    private ConnectionManager connectionManager;
    private ConnectionFactory connectionFactory;
    private RpcOption option;
    private Snowflake snowflake = IdUtil.getSnowflake(1, 1);

    @Override
    public void startup() throws LifeCycleException {
        super.startup();
        this.connectionFactory.startup();
        this.connectionManager.startup();
    }

    @Override
    public void init(RpcOption option) {
        this.option = option;
        this.connectionFactory = new DefaultConnectionFactory();
        this.connectionFactory.init(new ConnectionOption());
        this.connectionManager = new DefaultConnectionManager();
        this.connectionManager.init();
    }

    @Override
    public boolean createConnection(NodeId nodeId)
    {
        return getConnection(nodeId) != null;
    }

    @Override
    public Object invokeSync(NodeId nodeId, Object request) throws InterruptedException
    {
        return this.invokeSync(nodeId, request, option.getConnectTimeout());
    }

    @Override
    public Object invokeSync(NodeId nodeId, Object request, int timeoutMillis) throws InterruptedException
    {
        Connection connection = getConnection(nodeId);
        if (!connectionManager.check(connection))
        {
            return null;
        }

        long invokeId = snowflake.nextId();
        InvokeFuture invokeFuture = new InvokeFuture(invokeId);
        connection.addInvokeFuture(invokeFuture);
        connection.getChannel().writeAndFlush(new DefaultRpcCommand(invokeId, request));
        return invokeFuture.waitResult(timeoutMillis);
    }

    @Override
    public void invokeWithCallback(NodeId nodeId, Object request, InvokeCallback invokeCallback)
    {
        this.invokeWithCallback(nodeId, request, invokeCallback, option.getConnectTimeout());
    }

    @Override
    public void invokeWithCallback(NodeId nodeId, Object request, InvokeCallback invokeCallback, int timeoutMillis)
    {
        Connection connection = getConnection(nodeId);
        if (!connectionManager.check(connection))
        {
            return;
        }

        long invokeId = snowflake.nextId();
        InvokeFuture invokeFuture = new InvokeFuture(invokeId, invokeCallback);
        connection.addInvokeFuture(invokeFuture);
        connection.getChannel().writeAndFlush(new DefaultRpcCommand(invokeId, request));
    }

    @Override
    public void shutdown() throws LifeCycleException {
        super.shutdown();
        this.connectionFactory.shutdown();
        this.connectionManager.shutdown();
    }

    @Override
    public void invokeAsync(NodeId nodeId, Object request) {
        Connection connection = getConnection(nodeId);
        if (!connectionManager.check(connection))
        {
            return;
        }

        connection.getChannel().writeAndFlush(new DefaultRpcCommand(snowflake.nextId(), request));
    }

    private Connection getConnection(NodeId nodeId) {
        Connection connection = connectionManager.get(nodeId);
        if (connection == null) {

            List<Connection> connections = new ArrayList<>(10);
            for (int i = 0; i < 100; i++) {
                ChannelFuture future = connectionFactory.createConnection(nodeId.getIp(), nodeId.getPort(), option.getConnectTimeout());
                future.addListener(new GenericFutureListener<ChannelFuture>()
                {
                    @Override
                    public void operationComplete(ChannelFuture channelFuture) throws Exception
                    {
                        if (channelFuture.isSuccess()) {
                            Channel channel = channelFuture.channel();
                            connections.add(new Connection(channel, nodeId));
                        }
                    }
                });
            }

            connectionManager.put(nodeId, connections);
        }

        return connection;
    }
}
