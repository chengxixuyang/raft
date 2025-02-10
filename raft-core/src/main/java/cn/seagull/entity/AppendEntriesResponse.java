package cn.seagull.entity;

import lombok.Data;

@Data
public class AppendEntriesResponse
{
    private Boolean success;
    private Endpoint endpoint;
    private long term;
    private long lastLogIndex;
    private long commitLogIndex;
}
