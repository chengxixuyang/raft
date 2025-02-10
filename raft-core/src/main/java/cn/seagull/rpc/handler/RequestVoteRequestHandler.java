package cn.seagull.rpc.handler;

import cn.seagull.Node;
import cn.seagull.NodeManager;
import cn.seagull.SPI;
import cn.seagull.entity.RequestVoteRequest;
import cn.seagull.rpc.RaftService;
import lombok.extern.slf4j.Slf4j;

@SPI
@Slf4j
public class RequestVoteRequestHandler implements RpcHandler {

    @Override
    public boolean isAssignableFrom(Class<?> cls) {
        return RequestVoteRequest.class.isAssignableFrom(cls);
    }

    @Override
    public Object process(Object msg) {
        RequestVoteRequest request = (RequestVoteRequest) msg;

        Node node = NodeManager.getInstance().get(request.getEndpoint().getGroupId());
        RaftService raftService = (RaftService) node;

        if (raftService != null) {
            return raftService.handleRequestVoteRequest(request);
        }

        return null;
    }
}
