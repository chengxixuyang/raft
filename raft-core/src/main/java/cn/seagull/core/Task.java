package cn.seagull.core;

import cn.seagull.InvokeCallback;
import lombok.Data;

/**
 * @author yupeng
 */
@Data
public class Task
{
    private String groupId;

    private byte[] data;

    private InvokeCallback callback;
}
