package cn.seagull.storage.impl;

import cn.seagull.InvokeCallback;
import cn.seagull.Node;
import cn.seagull.entity.*;
import cn.seagull.rpc.RaftService;
import cn.seagull.rpc.RpcClient;
import cn.seagull.storage.LogManager;
import cn.seagull.storage.option.ReplicatorOption;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class NodeIdLogReplicate
{
    private LogManager logManager;
    private ScheduledExecutorService scheduledExecutorService;
    private RpcClient rpcClient;
    private LogIndex logIndex;
    private Endpoint targetId;
    private int delay;
    private ScheduledFuture<?> scheduledFuture;
    private Node node;
    private ScheduledFuture<?> heartbeatScheduledFuture;

    public NodeIdLogReplicate(ReplicatorOption option, LogIndex logIndex, Endpoint endpoint, Node node)
    {
        this.logManager = option.getLogManager();
        this.scheduledExecutorService = option.getScheduledExecutorService();
        this.node = node;
        this.rpcClient = option.getRpcClient();
        this.logIndex = logIndex;
        this.targetId = endpoint;
        this.delay = option.getElectionTimeoutMs() / 2;
    }

    public void start()
    {
        this.heartbeatScheduledFuture = this.scheduledExecutorService.scheduleWithFixedDelay(this::sendPing, 0, 100, TimeUnit.MILLISECONDS);
        this.scheduledFuture = this.scheduledExecutorService.scheduleWithFixedDelay(this::sendEntries, 0, delay, TimeUnit.MILLISECONDS);
    }

    public void cancel(boolean mayInterruptIfRunning)
    {
        if (heartbeatScheduledFuture != null) {
            this.heartbeatScheduledFuture.cancel(mayInterruptIfRunning);
        }

        if (scheduledFuture != null)
        {
            log.debug("from node {} to node log replicate {} is stop. ", this.node.getServerId(), this.targetId);
            this.scheduledFuture.cancel(mayInterruptIfRunning);
        }
    }

    public void updateLogIndex(LogIndex newLogIndex)
    {
        if (logIndex.getTerm() == newLogIndex.getTerm())
        {
            if (newLogIndex.getLastLogIndex() > logIndex.getLastLogIndex())
            {
//                log.debug("node {} update log index {}", this.targetId, newLogIndex);
                logIndex.setLastLogIndex(newLogIndex.getLastLogIndex());
            }
        }
        else if (logIndex.getTerm() < newLogIndex.getTerm())
        {
            logIndex.setTerm(newLogIndex.getTerm());
        }
    }

    public long getLastLogIndex()
    {
        return logIndex.getLastLogIndex();
    }

    private void sendPing() {
        this.rpcClient.invokeAsync(this.targetId.getServerId(), new PingRequest(this.targetId));
    }

    private void sendEntries()
    {
        try
        {
            long lastLogIndex = logManager.getLastLogIndex();
            long prevLogIndex = logIndex.getLastLogIndex();
            long nextLogIndex = prevLogIndex + 1;

            List<LogEntry> entries = new ArrayList<>();
            if (lastLogIndex >= nextLogIndex)
            {
                long count = lastLogIndex - nextLogIndex;
                List<Long> logIndexs = new ArrayList<>((int) count);
                for (long i = nextLogIndex; i <= lastLogIndex; i++)
                {
                    logIndexs.add(i);
                }
                entries = logManager.getEntry(logIndexs);
            }

            long term = logIndex.getTerm();

            AppendEntriesRequest request = new AppendEntriesRequest();
            request.setEndpoint(this.node.getServerId());
            request.setTerm(term);
            request.setPrevLogIndex(prevLogIndex);
            request.setPrevLogTerm(term);
            request.setEntries(entries);

//            log.debug("send node {}  entries {}.", targetId, entries.size());
            this.rpcClient.invokeWithCallback(this.targetId.getServerId(), request, new InvokeCallback()
            {
                @Override
                public void onSuccess(Object obj)
                {
                    ((RaftService)node).handleAppendEntriesResponse((AppendEntriesResponse) obj);
                }

                @Override
                public void onFail(Throwable e)
                {
                    log.error("send node {}  is fail {}.", targetId);
                }
            });
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
