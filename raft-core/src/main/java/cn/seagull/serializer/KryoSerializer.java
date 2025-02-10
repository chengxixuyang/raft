package cn.seagull.serializer;

import cn.hutool.core.io.IoUtil;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Pool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class KryoSerializer implements Serializer {

    private static final Pool<Kryo> pool = new Pool<Kryo>(true, false, 1024) {
        @Override
        protected Kryo create() {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(false);
            return kryo;
        }
    };

    @Override
    public byte[] serialize(Object obj) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Output output = new Output(outputStream);
        Kryo kryo = pool.obtain();
        try {
            kryo.writeClassAndObject(output, obj);
            output.flush();
            return outputStream.toByteArray();
        } finally {
            IoUtil.closeIfPosible(output);
            IoUtil.closeIfPosible(outputStream);
            pool.free(kryo);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes) {
        if (bytes == null)
            return null;

        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        Input input = new Input(inputStream);
        Kryo kryo = pool.obtain();
        try {
            return (T) kryo.readClassAndObject(input);
        } finally {
            IoUtil.closeIfPosible(input);
            IoUtil.closeIfPosible(inputStream);
            pool.free(kryo);
        }
    }
}
