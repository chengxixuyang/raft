package cn.seagull.core;

import cn.seagull.entity.Configuration;
import cn.seagull.entity.Endpoint;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * @author yupeng
 */
@Data
public class BallotBox
{
    private Configuration conf;
    private List<Endpoint> alreadyVotedNode = new CopyOnWriteArrayList<>();
    private Map<Endpoint, Long> alreadySubmissionsNodeMap = new ConcurrentHashMap<>();

    public BallotBox(Configuration conf)
    {
        this.conf = conf;
    }

    public boolean increaseNumberVotes(Endpoint endpoint)
    {
        if (conf.getConf().contains(endpoint) && !alreadyVotedNode.contains(endpoint))
        {
            alreadyVotedNode.add(endpoint);
            return true;
        }

        return false;
    }

    public long increaseNumberSubmissions(Endpoint endpoint, long commitLogIndex)
    {
        if (conf.getConf().contains(endpoint))
        {
            alreadySubmissionsNodeMap.put(endpoint, commitLogIndex);

            int half = conf.getConf().size() / 2;
            if (alreadySubmissionsNodeMap.size() >= half)
            {
                List<Long> commitLogIndexs = alreadySubmissionsNodeMap.values().stream().sorted().collect(Collectors.toList());
                return commitLogIndexs.get(half - 1);
            }
        }

        return -1;
    }

    public boolean isMoreThanHalf()
    {
        return alreadyVotedNode.size() > conf.getConf().size() / 2;
    }

    public void reset() {
        this.alreadyVotedNode.clear();
    }
}
