package cn.seagull.rpc;

import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author yupeng
 */
public class InvokeFutureContext
{
    private static final ConcurrentHashMap<Long, InvokeFuture> invokeFutureMap = new ConcurrentHashMap<Long, InvokeFuture>(8);

    public static InvokeFuture addInvokeFuture(InvokeFuture future) {
        return invokeFutureMap.putIfAbsent(future.getInvokeId(), future);
    }

    public static InvokeFuture removeInvokeFuture(Long id) {
        return invokeFutureMap.remove(id);
    }
}
