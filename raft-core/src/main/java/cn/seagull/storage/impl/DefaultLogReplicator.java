package cn.seagull.storage.impl;

import cn.seagull.AbstractLifeCycle;
import cn.seagull.LifeCycleException;
import cn.seagull.Node;
import cn.seagull.entity.Configuration;
import cn.seagull.entity.Endpoint;
import cn.seagull.entity.LogIndex;
import cn.seagull.storage.LogReplicator;
import cn.seagull.storage.option.ReplicatorOption;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
public class DefaultLogReplicator extends AbstractLifeCycle implements LogReplicator {

    private final Map<Endpoint, NodeIdLogReplicate> replicatorMap = new ConcurrentHashMap<>();
    private ReplicatorOption option;
    private Configuration conf;

    @Override
    public void init(ReplicatorOption option) {
        this.option = option;
        this.conf = option.getConf();
    }

    @Override
    public boolean addReplicator(Endpoint endpoint, Node node) {
        NodeIdLogReplicate logReplicate = replicatorMap.get(endpoint);
        if (logReplicate == null) {
            LogIndex logIndex = new LogIndex();
            logIndex.setTerm(option.getTerm());
            logIndex.setLastLogIndex(0);
            logReplicate = new NodeIdLogReplicate(option, logIndex, endpoint, node);
            logReplicate.start();
            replicatorMap.put(endpoint, logReplicate);
            return true;
        }

        return false;
    }

    @Override
    public void updateLogIndex(Endpoint endpoint, LogIndex logIndex) {
        NodeIdLogReplicate logReplicate = replicatorMap.get(endpoint);
        if (logReplicate != null) {
            logReplicate.updateLogIndex(logIndex);
        }
    }

    @Override
    public long getGroupFistLogIndex()
    {
        List<Long> lastLogIndexs = this.replicatorMap.entrySet().parallelStream().filter(replicateEntry -> this.conf.serverExists(replicateEntry.getKey()))
                    .map(Map.Entry::getValue).map(NodeIdLogReplicate::getLastLogIndex).sorted().toList();

        log.debug("last log index of group {}.", lastLogIndexs);
        return lastLogIndexs.getFirst();
    }

    @Override
    public boolean stopReplicator(Endpoint endpoint) {
        NodeIdLogReplicate logReplicate = replicatorMap.get(endpoint);
        if (logReplicate != null) {
            logReplicate.cancel(true);
            return true;
        }

        return false;
    }

    @Override
    public boolean resetTerm(long newTerm) {
        if (newTerm > option.getTerm()) {
            option.setTerm(newTerm);
            return true;
        }

        return false;
    }

    @Override
    public boolean contains(Endpoint endpoint) {
        return replicatorMap.containsKey(endpoint);
    }

    @Override
    public void stopAll() {
        replicatorMap.values().forEach(future -> future.cancel(true));
    }

    @Override
    public void shutdown() throws LifeCycleException
    {
        super.shutdown();

        this.stopAll();
    }
}
