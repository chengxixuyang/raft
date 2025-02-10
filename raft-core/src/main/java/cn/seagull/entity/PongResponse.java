package cn.seagull.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PongResponse {

    private Long sendTimestamp;

    public PongResponse(Long sendTimestamp) {
        this.sendTimestamp = sendTimestamp;
    }
}
