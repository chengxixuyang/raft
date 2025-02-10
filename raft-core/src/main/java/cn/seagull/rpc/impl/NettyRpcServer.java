package cn.seagull.rpc.impl;

import cn.hutool.core.util.StrUtil;
import cn.seagull.AbstractLifeCycle;
import cn.seagull.LifeCycleException;
import cn.seagull.SPI;
import cn.seagull.rpc.RpcChannelInitializer;
import cn.seagull.rpc.RpcServer;
import cn.seagull.rpc.option.RpcOption;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

@SPI
public class NettyRpcServer extends AbstractLifeCycle implements RpcServer {

    private ServerBootstrap serverBootstrap;

    private NioEventLoopGroup workGroup;

    private NioEventLoopGroup bossGroup;

    private RpcOption option;

    private ChannelFuture channelFuture;

    private String ip;

    private int port;

    @Override
    public void init(RpcOption rpcOption) {
        this.option = rpcOption;
        this.port = rpcOption.getPort();
        this.ip = StrUtil.isNotBlank(rpcOption.getIp()) ? rpcOption.getIp() : (new InetSocketAddress(port)).getAddress().getHostAddress();
        this.bossGroup = new NioEventLoopGroup(1);
        this.workGroup = new NioEventLoopGroup();
        this.serverBootstrap = new ServerBootstrap();
        this.serverBootstrap.group(bossGroup, workGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 2048)
                .option(NioChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(256 * 1024, 512 * 1024))
                .childHandler(new RpcChannelInitializer());
    }

    @Override
    public int port() {
        return this.port;
    }

    @Override
    public String ip() {
        return this.ip;
    }

    @Override
    public void startup() throws LifeCycleException {
        super.startup();

        try {
            this.channelFuture = this.serverBootstrap.bind(new InetSocketAddress(this.ip(), this.port())).sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (channelFuture.isSuccess() && this.port == 0) {
            InetSocketAddress localAddress = (InetSocketAddress) this.channelFuture.channel().localAddress();
            this.port = localAddress.getPort();
        }
    }

    @Override
    public void shutdown() throws LifeCycleException {
        super.shutdown();

        if (channelFuture != null) {
            channelFuture.channel().close();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }
}
