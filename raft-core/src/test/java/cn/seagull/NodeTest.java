package cn.seagull;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.thread.NamedThreadFactory;
import cn.hutool.core.util.RandomUtil;
import cn.seagull.core.NodeImpl;
import cn.seagull.core.NodeOption;
import cn.seagull.core.State;
import cn.seagull.core.Task;
import cn.seagull.entity.Configuration;
import cn.seagull.entity.Endpoint;
import cn.seagull.entity.NodeId;
import cn.seagull.rpc.RpcClient;
import cn.seagull.rpc.RpcServer;
import cn.seagull.rpc.option.RpcOption;
import cn.seagull.util.ServiceLoaderFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@Slf4j
public class NodeTest {

    public static final int INIT_PORT = 7010;

    public static void main(String[] args) throws InterruptedException {
        RpcServer rpcServer = null;
        RpcClient rpcClient = null;

        List<Node> nodes = new ArrayList<>(3);
        for (int i = 1; i <= 3; i++) {
            String groupId = "group-" + i;
            List<Endpoint> nodeIds = generateNodeIds(groupId, 3);

            Configuration conf = new Configuration();
            conf.setConf(nodeIds);
            Endpoint serverId = nodeIds.get(Integer.valueOf(args[0]));

            RpcOption rpcOption = new RpcOption();
            rpcOption.setIp(serverId.getServerId().getIp());
            rpcOption.setPort(serverId.getServerId().getPort());

            if (rpcClient == null) {
                rpcClient = ServiceLoaderFactory.loadFirstService(RpcClient.class);
                rpcClient.init(rpcOption);
                rpcClient.startup();
            }

            if (rpcServer == null) {
                rpcServer = ServiceLoaderFactory.loadFirstService(RpcServer.class);
                rpcServer.init(rpcOption);
                rpcServer.startup();
            }

            NodeOption option = new NodeOption();
            option.setRpcServer(rpcServer);
            option.setRpcClient(rpcClient);

            nodes.add(createNode(serverId, conf, option, groupId));
        }

        Thread.sleep(10000);

        for (Node node : nodes) {
            if (node.state() == State.LEADER) {
                for (int i = 0; i < 100000; i++) {
                    Task task = new Task();
                    task.setData(RandomUtil.randomBytes(128));
                    node.apply(task);
                }
            }
        }
    }

    public static Node createNode(Endpoint serverId, Configuration conf, NodeOption option, String groupId) {
        NodeImpl node = new NodeImpl(serverId);
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
        ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(10, new NamedThreadFactory("raft-schedule", true));
        option.setConnectTimeout(1000);
        option.setConf(conf);
        String url = Paths.get(System.getProperty("user.dir"), "/tmp/" + groupId, "raft_test_" + serverId.getServerId()
                .getPort()).toString();
        FileUtil.mkdir(new File(url));
        option.setLogUrl(url);
        option.setExecutorService(executorService);
        option.setScheduledExecutorService(scheduledExecutorService);

        node.init(option);
        node.startup();

        return node;
    }

    public static List<Endpoint> generateNodeIds(String groupId, int n) {
        List<Endpoint> result = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            result.add(createNodeId(groupId, i));
        }
        return result;
    }

    public static Endpoint createNodeId(String groupId, int i) {
        return new Endpoint(groupId, new NodeId("localhost", INIT_PORT + i));
    }

    public static String getIp() {
        String ip = null;
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                // filters out 127.0.0.1 and inactive interfaces
                if (iface.isLoopback() || !iface.isUp()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    if (addr instanceof Inet4Address) {
                        ip = addr.getHostAddress();
                        break;
                    }
                }
            }
            return ip;
        } catch (SocketException e) {
            return "localhost";
        }
    }
}
