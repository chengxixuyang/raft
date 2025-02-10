package cn.seagull.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yupeng
 */
@Data
@NoArgsConstructor
public class Endpoint
{
    private String groupId;

    private NodeId serverId;

    public Endpoint(String groupId, NodeId serverId)
    {
        this.groupId = groupId;
        this.serverId = serverId;
    }
}
