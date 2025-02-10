package cn.seagull.entity;

import lombok.Data;

import java.util.List;

@Data
public class AppendEntriesRequest
{
    private Endpoint endpoint;

    private long term;

    private long prevLogTerm;

    private long prevLogIndex;

    private long committedIndex;

    private List<LogEntry> entries;
}
