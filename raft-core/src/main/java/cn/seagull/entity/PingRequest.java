package cn.seagull.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PingRequest {

    private Long sendTimestamp = System.currentTimeMillis();

    private Endpoint targetId;

    public PingRequest(Endpoint targetId) {
        this.targetId = targetId;
    }
}
