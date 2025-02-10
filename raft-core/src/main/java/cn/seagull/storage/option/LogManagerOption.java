package cn.seagull.storage.option;

import cn.seagull.entity.Configuration;
import lombok.Data;

@Data
public class LogManagerOption {
    private String path;
    private boolean sync;
    private Configuration conf;
}
