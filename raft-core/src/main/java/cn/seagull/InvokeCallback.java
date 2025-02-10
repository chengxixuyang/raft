package cn.seagull;

public interface InvokeCallback {

    void onSuccess(Object obj);

    void onFail(Throwable e);
}
