package cn.seagull.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class NodeId {

    private String ip;

    private Integer port;

    public NodeId(String ip, Integer port) {
        this.ip = ip;
        this.port = port;
    }
}
