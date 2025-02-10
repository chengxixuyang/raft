package cn.seagull.entity;

import lombok.Data;

import java.util.List;

@Data
public class Configuration {

    private List<Endpoint> conf;

    public boolean serverExists(Endpoint endpoint) {
        return this.conf.contains(endpoint);
    }
}
