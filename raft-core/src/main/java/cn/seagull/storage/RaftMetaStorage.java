package cn.seagull.storage;

import cn.seagull.LifeCycle;
import cn.seagull.entity.Endpoint;
import cn.seagull.entity.NodeId;

public interface RaftMetaStorage extends LifeCycle {

    boolean setTerm(long term);

    long getTerm();

    boolean setVotedFor(Endpoint endpoint);

    Endpoint getVotedFor();

    boolean setTermAndVotedFor(long term, Endpoint endpoint);
}
