package cn.seagull.rpc;

import cn.seagull.entity.AppendEntriesRequest;
import cn.seagull.entity.AppendEntriesResponse;
import cn.seagull.entity.RequestVoteRequest;
import cn.seagull.entity.RequestVoteResponse;

public interface RaftService {

    RequestVoteResponse handleRequestVoteRequest(RequestVoteRequest request);

    AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest request);

    void handleAppendEntriesResponse(AppendEntriesResponse response);
}
