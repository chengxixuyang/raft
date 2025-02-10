package cn.seagull.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class LogId {

    private long index;

    private long term;

    public LogId(long index, long term) {
        this.index = index;
        this.term = term;
    }
}
