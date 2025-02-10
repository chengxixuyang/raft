package cn.seagull.rpc.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.lang.Assert;
import cn.seagull.AbstractLifeCycle;
import cn.seagull.LifeCycleException;
import cn.seagull.entity.NodeId;
import cn.seagull.rpc.Connection;
import cn.seagull.rpc.ConnectionManager;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DefaultConnectionManager extends AbstractLifeCycle implements ConnectionManager {

    private Map<NodeId, List<Connection>> connections;

    @Override
    public void init() {
        this.connections = new ConcurrentHashMap<>(8);
    }

    @Override
    public void add(Connection connection) {
        NodeId nodeId = connection.getNodeId();
        Channel channel = connection.getChannel();

        Assert.notNull(nodeId);
        Assert.notNull(channel);

        List<Connection> connectionList = connections.getOrDefault(nodeId, new ArrayList<>());
        connectionList.add(connection);
        connections.putIfAbsent(nodeId, connectionList);
    }

    @Override
    public void put(NodeId nodeId, List<Connection> connectionList) {
        Assert.notNull(nodeId);
        Assert.notEmpty(connectionList);

        this.connections.put(nodeId, connectionList);
    }

    @Override
    public Connection get(NodeId nodeId) {
        List<Connection> connectionList = connections.get(nodeId);
        if (CollectionUtil.isEmpty(connectionList))
            return null;

        for (Connection connection : connectionList) {
            if (connection.getChannel() != null && connection.getChannel().isActive() && connection.getChannel().isWritable()) {
                return connection;
            }
        }

        return null;
    }

    @Override
    public List<Connection> getAll() {
        return connections.values().parallelStream().flatMap(Collection::parallelStream).toList();
    }

    @Override
    public void remove(Connection connection) {
        NodeId nodeId = connection.getNodeId();
        Assert.notNull(nodeId);

        connection.getChannel().close();
        this.connections.get(nodeId).remove(connection);
    }

    @Override
    public void removeAll() {
        connections.values().parallelStream().flatMap(Collection::parallelStream).forEach(connection -> connection.getChannel().close());
        connections.clear();
    }

    @Override
    public boolean check(Connection connection) {
        if (connection == null) {
            log.warn("Connection is null when do check!");
            return false;
        } else if (connection.getChannel() != null && connection.getChannel().isActive()) {
            if (!connection.getChannel().isWritable()) {
                log.warn("Check connection failed for address: " + connection.getNodeId() + ", maybe write overflow!");
                return false;
            }
        } else {
            this.remove(connection);
            log.warn("Check connection failed for address: " + connection.getNodeId());
            return false;
        }
        return true;
    }

    @Override
    public void shutdown() throws LifeCycleException {
        super.shutdown();

        this.removeAll();
    }
}
