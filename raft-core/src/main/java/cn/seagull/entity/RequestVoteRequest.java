package cn.seagull.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class RequestVoteRequest
{

    private Endpoint endpoint;

    private Long term;

    private Long lastLogIndex;

    private Long lastLogTerm;
}
