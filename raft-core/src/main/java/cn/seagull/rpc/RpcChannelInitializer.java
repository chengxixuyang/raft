package cn.seagull.rpc;

import cn.seagull.rpc.codec.KryoDecoder;
import cn.seagull.rpc.codec.KryoEncoder;
import cn.seagull.rpc.handler.RaftServiceHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

public class RpcChannelInitializer extends ChannelInitializer<NioSocketChannel> {

    @Override
    protected void initChannel(NioSocketChannel channel) throws Exception {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(new KryoDecoder());
        pipeline.addLast(new KryoEncoder());
        pipeline.addLast(new IdleStateHandler(0, 0, 90000, TimeUnit.MILLISECONDS));
//        pipeline.addLast(new BufferMonitorHandler());
        pipeline.addLast(new RaftServiceHandler());
    }
}
