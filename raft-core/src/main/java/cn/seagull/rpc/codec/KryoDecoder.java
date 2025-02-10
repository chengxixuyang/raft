package cn.seagull.rpc.codec;

import cn.seagull.serializer.SerializerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class KryoDecoder extends ByteToMessageDecoder {

    private static final int BODY_LENTH = 4;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> list) throws Exception {
        if (byteBuf.readableBytes() < BODY_LENTH)
        {
            return;
        }

        byteBuf.markReaderIndex();
        int length = byteBuf.readInt();

        if (length <= 0) {
            ctx.close();
            return;
        }

        if (length > byteBuf.readableBytes())
        {
            byteBuf.resetReaderIndex();
            return;
        }

        long start = System.currentTimeMillis();
        byte[] array;
        if (byteBuf.hasArray()) {
            ByteBuf slice = byteBuf.slice(byteBuf.readerIndex(), length);
            array = slice.array();
            byteBuf.retain();
        } else {
            array = new byte[length];
            byteBuf.readBytes(array, 0, length);
        }

        Object message = SerializerFactory.getDefaultSerializer().deserialize(array);
        if (byteBuf.hasArray()) {
            byteBuf.release();
        }

        if (message != null)
        {
            list.add(message);
        }
    }
}
