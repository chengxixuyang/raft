package cn.seagull;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import cn.seagull.entity.NodeId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NodeManager {

    private static final NodeManager INSTANCE = new NodeManager();

    private final ConcurrentMap<String, Node> nodeMap = new ConcurrentHashMap<>();

    public static NodeManager getInstance() {
        return INSTANCE;
    }

    /**
     * Return true when RPC service is registered.
     */
    public boolean serverExists(String id) {
        return this.nodeMap.containsKey(id);
    }

    /**
     * Adds a node.
     */
    public boolean add(Node node) {
        if (serverExists(node.groupId())) {
            return false;
        }

        return this.nodeMap.putIfAbsent(node.groupId(), node) != null;
    }

    /**
     * Remove a node.
     */
    public boolean remove(Node node) {
        return this.nodeMap.remove(node.groupId(), node);
    }

    /**
     * Get node by groupId and peer.
     */
    public Node get(String id) {
        return this.nodeMap.get(id);
    }

    /**
     * Get all nodes
     */
    public List<Node> getAllNodes() {
        return this.nodeMap.values().stream().toList();
    }
}
