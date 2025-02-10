package cn.seagull.rpc.codec;

import cn.seagull.serializer.SerializerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class KryoEncoder extends MessageToByteEncoder<Object> {

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Object o, ByteBuf byteBuf) throws Exception {
        byte[] bytes = SerializerFactory.getDefaultSerializer().serialize(o);// 将对象转换为byte
        int length = bytes.length;// 读取消息的长度
        byteBuf.writeInt(length);
        byteBuf.writeBytes(bytes);
    }
}
