package cn.seagull.storage.option;

import cn.seagull.entity.Configuration;
import cn.seagull.rpc.RpcClient;
import cn.seagull.storage.LogManager;
import lombok.Builder;
import lombok.Data;

import java.util.concurrent.ScheduledExecutorService;

@Data
@Builder
public class ReplicatorOption {

    private Configuration conf;

    private long term;

    private RpcClient rpcClient;

    private LogManager logManager;

    private ScheduledExecutorService scheduledExecutorService;

    private int electionTimeoutMs;
}
