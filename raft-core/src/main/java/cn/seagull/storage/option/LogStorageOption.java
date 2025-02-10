package cn.seagull.storage.option;

import cn.seagull.entity.Configuration;
import lombok.Data;

/**
 * @author yupeng
 */
@Data
public class LogStorageOption
{
    private String path;
    private boolean sync;
    private Configuration conf;
}
