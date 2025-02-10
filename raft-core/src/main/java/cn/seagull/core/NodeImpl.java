package cn.seagull.core;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.thread.NamedThreadFactory;
import cn.hutool.core.util.ObjectUtil;
import cn.seagull.*;
import cn.seagull.entity.*;
import cn.seagull.rpc.RaftService;
import cn.seagull.rpc.RpcClient;
import cn.seagull.serializer.SerializerFactory;
import cn.seagull.storage.LogManager;
import cn.seagull.storage.LogReplicator;
import cn.seagull.storage.RaftMetaStorage;
import cn.seagull.storage.handler.LogEntryEvent;
import cn.seagull.storage.handler.LogEntryFactory;
import cn.seagull.storage.handler.LogEntryHandler;
import cn.seagull.storage.impl.DefaultLogReplicator;
import cn.seagull.storage.impl.LogManagerImpl;
import cn.seagull.storage.option.LogManagerOption;
import cn.seagull.storage.option.ReplicatorOption;
import cn.seagull.util.ServiceLoaderFactory;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class NodeImpl extends AbstractLifeCycle implements Node, RaftService {
    private volatile long currentTerm;
    private Endpoint serverId;
    private Endpoint leaderId;
    private Endpoint votedId;
    private State state;
    private RpcClient rpcClient;
    private NodeOption option;
    private ScheduledExecutorService scheduledExecutorService;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock writeLock = lock.writeLock();
    private final Lock readLock = lock.readLock();
    private RaftMetaStorage metaStorage;
    private LogManager logManager;
    private BallotBox ballotBox;
    private LogReplicator replicator;
    private ScheduledFuture<?> electionTimeoutTimer;
    private ExecutorService executorService;
    private Configuration conf;
    private volatile long lastLeaderTimestamp;
    private Caller caller;
    private Disruptor<LogEntryEvent> applyDisruptor;
    private RingBuffer<LogEntryEvent> applyQueue;
    private List<Endpoint> addingNewNodes = new CopyOnWriteArrayList<>();

    public NodeImpl(Endpoint serverId) {
        this.serverId = serverId;
        this.currentTerm = 0;
    }

    @Override
    public void init(NodeOption option) {
        this.option = option;

        this.conf = option.getConf();
        this.ballotBox = new BallotBox(option.getConf());

        this.rpcClient = option.getRpcClient();

        this.scheduledExecutorService = option.getScheduledExecutorService();
        this.executorService = option.getExecutorService();

        this.metaStorage = ServiceLoaderFactory.loadFirstService(RaftMetaStorage.class);

        this.logManager = new LogManagerImpl();
        LogManagerOption logManagerOption = new LogManagerOption();
        logManagerOption.setPath(option.getLogUrl());
        logManagerOption.setSync(true);
        logManagerOption.setConf(this.conf);
        this.logManager.init(logManagerOption);

        this.replicator = new DefaultLogReplicator();
        this.replicator.init(ReplicatorOption.builder().conf(option.getConf()).term(this.currentTerm)
                .rpcClient(this.rpcClient).scheduledExecutorService(option.getScheduledExecutorService())
                .logManager(this.logManager).electionTimeoutMs(option.getElectionTimeoutMs()).build());

        this.applyDisruptor = new Disruptor<LogEntryEvent>(new LogEntryFactory(), this.option.getDisruptorBufferSize(), new NamedThreadFactory("Raft-NodeImpl-Disruptor-", true), ProducerType.MULTI, new BlockingWaitStrategy());
        this.applyDisruptor.handleEventsWith(new LogEntryHandler(this.option.getApplyBatch(), this.option.getElectionTimeoutMs() / 2, this.logManager));

        this.caller = new DefaultCaller();
        CallerOption callerOption = new CallerOption();
        callerOption.setLogManager(this.logManager);
        this.caller.init(callerOption);
    }

    @Override
    public NodeOption nodeOption() {
        return this.option;
    }

    @Override
    public String groupId() {
        return this.serverId.getGroupId();
    }

    @Override
    public void startup() throws LifeCycleException {
        super.startup();
        this.metaStorage.startup();
        this.replicator.startup();
        this.logManager.startup();
        this.applyQueue = this.applyDisruptor.start();
        this.currentTerm = this.metaStorage.getTerm();
        this.votedId = ObjectUtil.cloneByStream(this.metaStorage.getVotedFor());
        NodeManager.getInstance().add(this);
        this.lastLeaderTimestamp = System.currentTimeMillis();
        this.state = State.FOLLOWER;
        handleElectionTimeout();
    }

    @Override
    public void shutdown() throws LifeCycleException {
        super.shutdown();
        this.metaStorage.shutdown();
        this.replicator.shutdown();
        this.logManager.shutdown();
    }

    @Override
    public Endpoint getServerId() {
        this.readLock.lock();
        try {
            if (isStarted()) {
                return this.serverId;
            }
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    @Override
    public void apply(Task task) {
        if (!isStarted()) {
            return;
        }

        Assert.notNull(task, "Null task");

        if (state != State.LEADER) {
            rpcClient.invokeAsync(this.leaderId.getServerId(), task);
            return;
        }

        LogEntry entry = new LogEntry();
        entry.setData(task.getData());

        EventTranslator<LogEntryEvent> translator = (event, sequence) -> {
            event.setCallback(task.getCallback());
            event.setEntry(entry);
            event.setCurrTerm(currentTerm);
        };

        applyQueue.publishEvent(translator);
    }

    @Override
    public State state() {
        this.readLock.lock();
        try {
            return this.state;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        return this.logManager.getLastLogIndex();
    }

    @Override
    public long getLastCommittedIndex() {
        this.readLock.lock();
        try {
            return this.caller.getLastCommittedIndex();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void resetLastLeaderTimestamp(long lastLeaderTimestamp) {
        this.lastLeaderTimestamp = lastLeaderTimestamp;
    }

    @Override
    public void addNode(Endpoint endpoint) {
        Assert.notNull(endpoint, "Null node");

        writeLock.lock();
        try {
            if (state != State.LEADER) {
                rpcClient.invokeAsync(this.leaderId.getServerId(), new AddNode(endpoint));
                return;
            }

            if (!conf.serverExists(endpoint)) {
                if (!replicator.addReplicator(endpoint, NodeImpl.this)) {
                    log.error("Fail to add a replicator, peer={}.", endpoint);
                }

                addingNewNodes.add(endpoint);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void removeNode(Endpoint endpoint) {
        Assert.notNull(endpoint, "Null node");

        writeLock.lock();
        try {
            if (state != State.LEADER) {
                rpcClient.invokeAsync(this.leaderId.getServerId(), new RemoveNode(endpoint));
                return;
            }

            if (conf.serverExists(endpoint)) {
                if (!replicator.stopReplicator(endpoint)) {
                    log.error("Fail to stop a replicator, nodeId={}.", endpoint);
                }

                conf.getConf().remove(endpoint);
                appendConfEntry();
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void handleElectionTimeout() {
        this.electionTimeoutTimer = this.scheduledExecutorService.scheduleWithFixedDelay(this::preVote, 1000, ThreadLocalRandom.current()
                .nextInt(this.option.getElectionTimeoutMs(), this.option.getElectionTimeoutMs() + this.option.getMaxElectionDelayMs()), TimeUnit.MILLISECONDS);
    }

    private void preVote() {
        this.readLock.lock();
        long oldTerm;
        try {
            if (!this.conf.serverExists(this.serverId)) {
                log.warn("Node {} can't do preVote as it is not in conf <{}>.", getServerId(), this.conf);
                return;
            }

            if (isCurrentLeaderValid()) {
                return;
            }

            oldTerm = this.currentTerm;
        } finally {
            this.readLock.unlock();
        }

        LogId lastLogId = this.logManager.getLastLogId();

        this.writeLock.lock();
        try {
            if (oldTerm != this.currentTerm) {
                log.warn("Node {} raise term {} when get lastLogId.", getServerId(), this.currentTerm);
                return;
            }

            this.state = State.CANDIDATE;
            this.leaderId = null;
            this.ballotBox.increaseNumberVotes(this.serverId);
            this.currentTerm++;

            if (this.conf.getConf().size() == 1) {
                becomeFollower(this.currentTerm);
                return;
            }

            for (Endpoint endpoint : this.conf.getConf()) {
                if (Objects.equals(endpoint, this.serverId)) {
                    continue;
                }

                if (!this.rpcClient.createConnection(endpoint.getServerId())) {
                    log.warn("Node {} channel init failed, address={}.", getServerId(), endpoint);
                    continue;
                }

                RequestVoteRequest request = new RequestVoteRequest();
                request.setEndpoint(this.serverId);
                request.setTerm(currentTerm);
                request.setLastLogIndex(lastLogId.getIndex());
                request.setLastLogTerm(lastLogId.getTerm());

                this.rpcClient.invokeWithCallback(endpoint.getServerId(), request, new InvokeCallback() {
                    @Override
                    public void onSuccess(Object obj) {
                        handleRequestVoteResponse((RequestVoteResponse) obj);
                    }

                    @Override
                    public void onFail(Throwable e) {
                        log.error("send node {} pre vote is fail {}.", endpoint);
                    }
                });
            }

            log.info("Node {} term {} start preVote.", getServerId(), this.currentTerm);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public RequestVoteResponse handleRequestVoteRequest(RequestVoteRequest request) {
        writeLock.lock();
        try {
            if (!isStarted()) {
                log.warn("Node {} is not in active state, currTerm={}.", getServerId(), currentTerm);
            }

            Endpoint endpoint = request.getEndpoint();
            NodeId candidateId = endpoint.getServerId();

            boolean granted = false;

            if (!conf.getConf().contains(endpoint)) {
                log.warn("Node {} ignore PreVoteRequest from {} as it is not in conf <{}>.", this.serverId, candidateId, conf);
            } else if (state == State.LEADER) {
                replicator.addReplicator(endpoint, this);
            } else if (isCurrentLeaderValid()) {
                log.warn("Node {} ignore PreVoteRequest from {} as it is leader is valid.", this.serverId, endpoint);
            } else if (request.getTerm() >= currentTerm) {
                log.info("Node {} received RequestVoteRequest from {}, term={}, currTerm={}.", this.serverId, candidateId, request.getTerm(), currentTerm);

                long lastLogIndex = logManager.getLastLogIndex();
                if (votedId == null && request.getLastLogIndex() >= lastLogIndex) {
                    granted = true;
                    votedId = endpoint;
                    metaStorage.setVotedFor(endpoint);
                    becomeFollower(request.getTerm());
                }
            }

            RequestVoteResponse response = new RequestVoteResponse();
            response.setTerm(currentTerm);
            response.setGranted(granted);
            response.setEndpoint(this.serverId);

            return response;
        } finally {
            writeLock.unlock();
        }
    }

    public void handleRequestVoteResponse(RequestVoteResponse response) {
        writeLock.lock();
        try {
            if (state != State.CANDIDATE) {
                log.warn("Node {} received invalid PreVoteResponse from {}, state not in FOLLOWER but {}.", getServerId(), response.getEndpoint(), state);
                return;
            }

            if (response.getTerm() > currentTerm) {
                log.warn("Node {} received invalid PreVoteResponse from {}, term {}, expect={}.", getServerId(), response.getEndpoint(), response.getTerm(), currentTerm);
                becomeFollower(response.getTerm());
                return;
            }

            log.info("Node {} received PreVoteResponse from {}, term={}, granted={}.", getServerId(), response.getEndpoint(), response.getTerm(), response.isGranted());

            if (response.isGranted()) {
                ballotBox.increaseNumberVotes(response.getEndpoint());

                if (ballotBox.isMoreThanHalf()) {
                    becomeLeader();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public AppendEntriesResponse handleAppendEntriesRequest(AppendEntriesRequest request) {
        writeLock.lock();
        try {
            if (!isStarted()) {
                log.warn("Node {} is not in active state, currTerm={}.", serverId, currentTerm);
                return null;
            }

            Endpoint endpoint = request.getEndpoint();

            // Check stale term
            if (request.getTerm() < currentTerm) {
                log.warn("Node {} ignore stale AppendEntriesRequest from {}, term={}, currTerm={}.", serverId, endpoint, request.getTerm(), currentTerm);
                return null;
            }

            if (request.getTerm() > currentTerm || state != State.FOLLOWER) {
                becomeFollower(request.getTerm());
                return null;
            }

            // save current leader
            if (leaderId == null) {
                leaderId = endpoint;
            }

            if (!Objects.equals(endpoint, leaderId)) {
                log.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.", endpoint, currentTerm, leaderId);

                becomeFollower(request.getTerm() + 1);
                return null;
            }

            long lastLogIndex = logManager.getLastLogIndex();

            AppendEntriesResponse response = new AppendEntriesResponse();
            response.setSuccess(false);
            response.setTerm(currentTerm);
            response.setEndpoint(this.serverId);
            response.setLastLogIndex(lastLogIndex);

            lastLeaderTimestamp = System.currentTimeMillis();

            List<LogEntry> entries = request.getEntries();
            long prevLogIndex = request.getPrevLogIndex();
            long prevLogTerm = request.getPrevLogTerm();
            long localPrevLogTerm = logManager.getTerm(prevLogIndex);

            if (localPrevLogTerm != -1 && localPrevLogTerm != prevLogTerm) {
                log.warn("Node {} reject term_unmatched AppendEntriesRequest from {}, term={}, prevLogIndex={}, prevLogTerm={}, localPrevLogTerm={}, lastLogIndex={}, entries={}.", this.serverId, request.getEndpoint(), request.getTerm(), prevLogIndex, prevLogTerm, localPrevLogTerm, lastLogIndex, entries);
                return response;
            }

            if (CollectionUtil.isEmpty(entries)) {
                return response;
            }

            response.setSuccess(true);

            logManager.appendEntries(new ArrayList<>(entries));
            lastLogIndex = logManager.getLastLogIndex();
            response.setLastLogIndex(lastLogIndex);

            caller.commit(lastLogIndex);
            response.setCommitLogIndex(lastLogIndex);

            return response;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            writeLock.unlock();
        }

        return null;
    }

    public void handleAppendEntriesResponse(AppendEntriesResponse response) {
        writeLock.lock();
        try {
            if (state != State.LEADER) {
                log.warn("Node {} received invalid handleAppendEntriesResponse from {}, state not in FOLLOWER but {}.", getServerId(), response.getEndpoint(), state);
                return;
            }

            if (response.getTerm() > currentTerm) {
                log.warn("Node {} received invalid handleAppendEntriesResponse from {}, term {}, expect={}.", getServerId(), response.getEndpoint(), response.getTerm(), currentTerm);
                becomeFollower(response.getTerm());
                return;
            }

            Endpoint endpoint = response.getEndpoint();

            if (response.getSuccess()) {
                long commitLogIndex = ballotBox.increaseNumberSubmissions(endpoint, response.getCommitLogIndex());
                if (commitLogIndex != -1) {
                    caller.commit(commitLogIndex);
                }
            }

            if (addingNewNodes.contains(endpoint) && response.getLastLogIndex() >= replicator.getGroupFistLogIndex()) {
                conf.getConf().add(endpoint);
                appendConfEntry();
                addingNewNodes.remove(endpoint);
            }

            LogIndex logIndex = new LogIndex();
            logIndex.setTerm(response.getTerm());
            logIndex.setLastLogIndex(response.getLastLogIndex());

            replicator.updateLogIndex(endpoint, logIndex);
        } finally {
            writeLock.unlock();
        }
    }

    private void becomeLeader() {
        Assert.isTrue(this.state == State.CANDIDATE, "Illegal state: " + this.state);
        log.info("Node {} become leader of group, term={}", getServerId(), this.currentTerm);

        // cancel candidate vote timer
        this.state = State.LEADER;
        this.leaderId = ObjectUtil.cloneByStream(this.serverId);
        this.replicator.resetTerm(this.currentTerm);
        this.stopElectionTimeoutTimer();

        appendConfEntry();

        // Start follower's replicators
        for (Endpoint endpoint : this.conf.getConf()) {
            if (Objects.equals(endpoint, this.serverId)) {
                continue;
            }

            log.debug("Node {} add a replicator, term={}, node={}.", getServerId(), this.currentTerm, endpoint);
            if (!this.replicator.addReplicator(endpoint, this)) {
                log.error("Fail to add a replicator, peer={}.", endpoint);
            }
        }

        this.ballotBox.reset();
    }

    private void becomeFollower(long term) {
        log.debug("Node {} stepDown, term={}, newTerm={}.", getServerId(), this.currentTerm, term);
        if (!this.isStarted()) {
            return;
        }

        // reset leader_id
        this.leaderId = null;
        this.lastLeaderTimestamp = System.currentTimeMillis();
        this.state = State.FOLLOWER;

        // meta state
        if (term > this.currentTerm) {
            log.debug("Node {} set newTerm{}, oldTerm={}.", getServerId(), term, this.currentTerm);
            this.currentTerm = term;
            this.votedId = null;
            this.metaStorage.setTermAndVotedFor(term, this.votedId);
        }

        this.replicator.stopAll();
        this.ballotBox.reset();
    }

    private void stopElectionTimeoutTimer() {
        if (this.electionTimeoutTimer != null) {
            this.electionTimeoutTimer.cancel(true);
        }
    }

    private boolean isCurrentLeaderValid() {
        return System.currentTimeMillis() - this.lastLeaderTimestamp < this.option.getElectionTimeoutMs();
    }

    private void appendConfEntry() {
        LogId logId = new LogId(0, this.currentTerm);
        LogEntry entry = new LogEntry();
        entry.setType(EntryType.CONFIGURATION);
        entry.setId(logId);
        entry.setData(SerializerFactory.getDefaultSerializer().serialize(this.conf));
        this.logManager.appendEntries(Arrays.asList(entry));
    }
}
