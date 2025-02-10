package cn.seagull.rpc;

import cn.seagull.LifeCycle;
import cn.seagull.entity.NodeId;

import java.util.List;

public interface ConnectionManager extends LifeCycle {

    void init();

    void add(Connection connection);

    void put(NodeId nodeId, List<Connection> connections);

    Connection get(NodeId nodeId);

    List<Connection> getAll();

    void remove(Connection connection);

    void removeAll();

    boolean check(Connection connection);
}
