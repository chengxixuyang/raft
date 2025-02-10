package cn.seagull.rpc.handler;

import cn.seagull.Node;
import cn.seagull.NodeManager;
import cn.seagull.SPI;
import cn.seagull.entity.AppendEntriesRequest;
import cn.seagull.rpc.RaftService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SPI
public class AppendEntriesRequestHandler implements RpcHandler {

    @Override
    public boolean isAssignableFrom(Class<?> cls) {
        return AppendEntriesRequest.class.isAssignableFrom(cls);
    }

    @Override
    public Object process(Object msg) {
        AppendEntriesRequest request = (AppendEntriesRequest) msg;

        Node node = NodeManager.getInstance().get(request.getEndpoint().getGroupId());
        RaftService raftService = (RaftService) node;

        if (raftService != null) {
            return raftService.handleAppendEntriesRequest(request);
        }

        return null;
    }
}
