package cn.seagull.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class RequestVoteResponse
{

    private Long term;

    private boolean granted;

    private Endpoint endpoint;
}
