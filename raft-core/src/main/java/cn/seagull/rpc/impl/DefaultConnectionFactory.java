package cn.seagull.rpc.impl;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.thread.NamedThreadFactory;
import cn.seagull.AbstractLifeCycle;
import cn.seagull.LifeCycleException;
import cn.seagull.entity.NodeId;
import cn.seagull.rpc.ConnectionFactory;
import cn.seagull.rpc.RpcChannelInitializer;
import cn.seagull.rpc.option.ConnectionOption;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;


public class DefaultConnectionFactory extends AbstractLifeCycle implements ConnectionFactory {

    private Bootstrap bootstrap;

    private static final EventLoopGroup workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() + 1, new NamedThreadFactory("netty-client-worker", true));

    private ConnectionOption option;

    @Override
    public void init(ConnectionOption option) {
        this.option = option;
        this.bootstrap = new Bootstrap();
        this.bootstrap
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(256 * 1024, 512 * 1024))
                .handler(new RpcChannelInitializer());
    }

    @Override
    public ChannelFuture createConnection(NodeId nodeId) throws Exception {
        Assert.notNull(nodeId);
        return createConnection(nodeId.getIp(), nodeId.getPort(), option.getConnectTimeout());
    }

    @Override
    public ChannelFuture createConnection(String targetIp, int targetPort, int connectTimeout) {
        Assert.notBlank(targetIp);
        Assert.notNull(targetPort);
        Assert.notNull(connectTimeout);

        this.bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
        ChannelFuture future = this.bootstrap.connect(new InetSocketAddress(targetIp, targetPort));

        return future;
    }

    @Override
    public void shutdown() throws LifeCycleException {
        super.shutdown();

        workerGroup.shutdownGracefully();
    }
}
