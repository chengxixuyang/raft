package cn.seagull.rpc.handler;

public interface RpcHandler {

    boolean isAssignableFrom(Class<?> cls);

    Object process(Object command);
}
