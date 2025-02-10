package cn.seagull.rpc;

import cn.seagull.InvokeCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author yupeng
 */
public class InvokeFuture<T>
{
    private long invokeId;
    private T result;
    private Throwable cause;
    private CountDownLatch latch;

    private InvokeCallback callback;

    public InvokeFuture(long invokeId)
    {
        this(invokeId, null);
    }

    public InvokeFuture(long invokeId, InvokeCallback callback)
    {
        this.latch = new CountDownLatch(1);
        this.invokeId = invokeId;
        this.callback = callback;
    }

    public void setResult(T obj)
    {
        this.result = obj;
        if (callback != null && obj != null) {
            callback.onSuccess(obj);
        }

        latch.countDown();
    }

    public T waitResult(long timeoutMillis) throws InterruptedException {
        this.latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.result;
    }

    public T waitResult() throws InterruptedException {
        this.latch.await();
        return this.result;
    }

    public long getInvokeId()
    {
        return invokeId;
    }

    public T getResult()
    {
        return result;
    }

    public Throwable getException()
    {
        return cause;
    }

    public void setCause(Throwable cause)
    {
        this.cause = cause;
    }
}
