package cn.seagull.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class LogIndex {

    private long term;

    private long lastLogIndex;
}
