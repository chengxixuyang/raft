package cn.seagull.core;

import cn.seagull.entity.Configuration;
import cn.seagull.rpc.RpcClient;
import cn.seagull.rpc.RpcServer;
import lombok.Data;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

@Data
public class NodeOption
{
    private int electionTimeoutMs = 1000;
    private int maxElectionDelayMs = 500;
    private int connectTimeout;
    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private Configuration conf;
    private RpcServer rpcServer;
    private RpcClient rpcClient;
    private int disruptorBufferSize = 16384;
    private int applyBatch = 65536;
    private String logUrl;
}
