package cn.seagull.rpc.handler;

import cn.seagull.rpc.RpcCommand;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class RaftServiceHandler extends ChannelDuplexHandler {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg == null) {
            super.channelRead(ctx, msg);
            return;
        }

        if (msg instanceof RpcCommand command)
        {
            BaseRpcHandler.getInstance().handler(ctx, command);
        }

        super.channelRead(ctx, msg);
    }

}
