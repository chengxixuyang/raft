package cn.seagull.rpc.option;

import lombok.Data;

@Data
public class RpcOption {

    private int port;

    private String ip;

    private int connectTimeout = 1000;
}
