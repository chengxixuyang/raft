package cn.seagull.serializer;


public class SerializerFactory {

    public static Serializer getDefaultSerializer() {
        return new KryoSerializer();
    }

}
