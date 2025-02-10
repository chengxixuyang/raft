package cn.seagull.rpc.handler;

import io.netty.channel.*;
import lombok.extern.slf4j.Slf4j;

/**
 * @author yupeng
 */
@Slf4j
@ChannelHandler.Sharable
public class BufferMonitorHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // 输出初始配置
        WriteBufferWaterMark waterMark = ctx.channel().config().getWriteBufferWaterMark();
        log.debug("[Initial Config] Low: " + waterMark.low() + ", High: " + waterMark.high());
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        // 可写状态变化时打印实时状态
        Channel channel = ctx.channel();
        log.debug("[Real-Time] Writable: " + channel.isWritable() + ", Bytes Before Unwritable: " + channel.bytesBeforeUnwritable());

        ctx.fireChannelWritabilityChanged();
    }
}
