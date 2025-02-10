package cn.seagull.storage;

import cn.seagull.LifeCycle;
import cn.seagull.Node;
import cn.seagull.entity.Endpoint;
import cn.seagull.entity.LogIndex;
import cn.seagull.entity.NodeId;
import cn.seagull.rpc.RaftService;
import cn.seagull.storage.option.ReplicatorOption;

public interface LogReplicator extends LifeCycle {

    void init(ReplicatorOption option);

    boolean addReplicator(Endpoint endpoint, Node node);

    void updateLogIndex(Endpoint endpoint, LogIndex logIndex);

    long getGroupFistLogIndex();

    boolean stopReplicator(Endpoint endpoint);

    boolean resetTerm(long newTerm);

    boolean contains(Endpoint endpoint);

    void stopAll();
}
