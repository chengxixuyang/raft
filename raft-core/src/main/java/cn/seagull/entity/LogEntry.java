package cn.seagull.entity;

import lombok.Data;

@Data
public class LogEntry
{

    private EntryType type;

    private LogId id;

    private byte[] data;
}
