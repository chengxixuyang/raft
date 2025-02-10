package cn.seagull.rpc;

import cn.seagull.LifeCycle;
import cn.seagull.rpc.option.RpcOption;

public interface RpcServer extends LifeCycle {

    void init(RpcOption rpcOption);

    int port();

    String ip();
}
