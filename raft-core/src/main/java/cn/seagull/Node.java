package cn.seagull;

import cn.seagull.core.Task;
import cn.seagull.entity.Endpoint;
import cn.seagull.entity.NodeId;
import cn.seagull.core.NodeOption;
import cn.seagull.core.State;

public interface Node extends LifeCycle {

    void init(NodeOption option);

    NodeOption nodeOption();

    String groupId();

    Endpoint getServerId();

    void apply(Task task);

    State state();

    long getLastLogIndex();

    long getLastCommittedIndex();

    void resetLastLeaderTimestamp(long lastLeaderTimestamp);

    void addNode(Endpoint endpoint);

    void removeNode(Endpoint endpoint);
}
