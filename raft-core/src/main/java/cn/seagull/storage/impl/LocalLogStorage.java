package cn.seagull.storage.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ByteUtil;
import cn.seagull.AbstractLifeCycle;
import cn.seagull.LifeCycleException;
import cn.seagull.SPI;
import cn.seagull.entity.Configuration;
import cn.seagull.entity.EntryType;
import cn.seagull.entity.LogEntry;
import cn.seagull.serializer.SerializerFactory;
import cn.seagull.storage.LogStorage;
import cn.seagull.storage.option.LogStorageOption;
import io.netty.util.internal.SystemPropertyUtil;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * @author yupeng
 */
@SPI(priority = 5)
@Slf4j
public class LocalLogStorage extends AbstractLifeCycle implements LogStorage
{
    private String path;
    private RocksDB rocksDB;
    private WriteOptions writeOptions;
    private ReadOptions readOptions;
    private ColumnFamilyHandle defaultHandle;
    private ColumnFamilyHandle confHandle;
    private LogStorageOption option;
    private boolean sync;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    private final byte[] FIRST_LOG_INDEX_KEY = "FIRST_LOG_INDEX".getBytes(StandardCharsets.UTF_8);
    private final byte[] LAST_LOG_INDEX_KEY = "LAST_LOG_INDEX".getBytes(StandardCharsets.UTF_8);
    private final byte[] APPLIED_LOG_INDEX_KEY = "APPLIED_LOG_INDEX_KEY".getBytes(StandardCharsets.UTF_8);

    @Override
    public void init(LogStorageOption option)
    {
        writeLock.lock();
        try
        {
            this.option = option;
            this.path = option.getPath();
            this.sync = option.isSync();

            DBOptions dbOptions = new DBOptions();
            dbOptions.setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .setMaxOpenFiles(-1)
                    .setKeepLogFileNum(100)
                    .setMaxTotalWalSize(1 << 30)
                    .setAllowMmapReads(true)
                    .setMaxBackgroundCompactions(Math.min(Runtime.getRuntime().availableProcessors(), 4))
                    .setMaxBackgroundJobs(4);

            this.writeOptions = new WriteOptions();
            this.writeOptions.setSync(this.sync);

            this.readOptions = new ReadOptions();
            this.readOptions.setTotalOrderSeek(true);

            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
            tableConfig.setIndexType(IndexType.kTwoLevelIndexSearch)
                    .setFilterPolicy(new BloomFilter((double) 16, false)).setPartitionFilters(true)
                    .setMetadataBlockSize(8 * SizeUnit.KB).setCacheIndexAndFilterBlocks(false)
                    .setCacheIndexAndFilterBlocksWithHighPriority(true).setPinL0FilterAndIndexBlocksInCache(true)
                    .setBlockSize(4 * SizeUnit.KB).setBlockCacheSize(512 * SizeUnit.MB).setCacheNumShardBits(8);

            ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

            columnFamilyOptions.setWriteBufferSize(64 * SizeUnit.MB).setMaxWriteBufferNumber(3)
                    .setMinWriteBufferNumberToMerge(1).setLevel0FileNumCompactionTrigger(10)
                    .setLevel0SlowdownWritesTrigger(20).setLevel0StopWritesTrigger(40)
                    .setMaxBytesForLevelBase(512 * SizeUnit.MB).setTargetFileSizeBase(64 * SizeUnit.MB)
                    .setMemtablePrefixBloomSizeRatio(0.125).setForceConsistencyChecks(true)
                    .useFixedLengthPrefixExtractor(8).setTableFormatConfig(tableConfig)
                    .setMergeOperator(new StringAppendOperator());

            if (!SystemPropertyUtil.get("os.name", "").toLowerCase(Locale.US).contains("win"))
            {
                columnFamilyOptions.setCompressionType(CompressionType.LZ4_COMPRESSION)
                        .setCompactionStyle(CompactionStyle.LEVEL).optimizeLevelStyleCompaction();
            }

            List<ColumnFamilyOptions> cfOptions = new ArrayList<>();
            cfOptions.add(columnFamilyOptions);

            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor("Configuration".getBytes(), columnFamilyOptions));
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));

            final File dir = new File(this.path);
            if (dir.exists() && !dir.isDirectory())
            {
                throw new IllegalStateException("Invalid log path, it's a regular file: " + this.path);
            }

            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

            this.rocksDB = RocksDB.open(dbOptions, this.path, columnFamilyDescriptors, columnFamilyHandles);
            this.confHandle = columnFamilyHandles.get(0);
            this.defaultHandle = columnFamilyHandles.get(1);

            checkState();

            RocksIterator rocksIterator = this.rocksDB.newIterator();
            rocksIterator.seekToFirst();
            while (rocksIterator.isValid()) {
                byte[] key = rocksIterator.key();
                log.debug("load log index {}.", ByteUtil.bytesToLong(key));
                rocksIterator.next();
            }
        }
        catch (RocksDBException e)
        {
            throw new RuntimeException(e);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long getFirstLogIndex()
    {
        readLock.lock();
        checkState();
        try
        {
            byte[] bytes = this.rocksDB.get(this.confHandle, this.readOptions, FIRST_LOG_INDEX_KEY);
            if (bytes != null) {
                return ByteUtil.bytesToLong(bytes);
            }
            return 0;
        }
        catch (RocksDBException e)
        {
            throw new RuntimeException(e);
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex()
    {
        readLock.lock();
        checkState();
        try
        {
            byte[] bytes = this.rocksDB.get(this.confHandle, this.readOptions, LAST_LOG_INDEX_KEY);
            if (bytes != null) {
                return ByteUtil.bytesToLong(bytes);
            }
            return 0;
        }
        catch (RocksDBException e)
        {
            throw new RuntimeException(e);
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(long index)
    {
        readLock.lock();
        checkState();
        try
        {
            byte[] bytes = this.rocksDB.get(this.defaultHandle, this.readOptions, ByteUtil.longToBytes(index));
            LogEntry entry = SerializerFactory.getDefaultSerializer().deserialize(bytes);
            if (entry != null) {
                return entry;
            }

            return null;
        }
        catch (RocksDBException e)
        {
            throw new RuntimeException(e);
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public List<LogEntry> getEntry(List<Long> logIndexs)
    {
        if (CollectionUtil.isEmpty(logIndexs)) {
            return null;
        }

        readLock.lock();
        checkState();
        try
        {
            List<byte[]> bytes = this.rocksDB.multiGetAsList(this.readOptions, logIndexs.stream()
                    .map(ByteUtil::longToBytes).collect(Collectors.toList()));

            if (CollectionUtil.isNotEmpty(bytes)) {
                return bytes.parallelStream().map(entry -> SerializerFactory.getDefaultSerializer().<LogEntry>deserialize(entry)).filter(Objects::nonNull).collect(Collectors.toList());
            }

            return null;
        }
        catch (RocksDBException e)
        {
            throw new RuntimeException(e);
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public int appendEntries(List<LogEntry> entries)
    {
        if (CollectionUtil.isEmpty(entries)) {
            return 0;
        }

        readLock.lock();
        try(WriteBatch batch = new WriteBatch())
        {
            int count = entries.size();

            for (LogEntry entry : entries)
            {
                byte[] logIndex = ByteUtil.longToBytes(entry.getId().getIndex());
                byte[] data = SerializerFactory.getDefaultSerializer().serialize(entry);

                batch.put(this.defaultHandle, logIndex, data);

                if (entry.getType() == EntryType.CONFIGURATION) {
                    Configuration configuration = SerializerFactory.getDefaultSerializer().deserialize(entry.getData());
                    if (!Objects.equals(configuration, this.option.getConf())) {
                        this.option.setConf(configuration);
                    }
                }
            }

            this.rocksDB.write(this.writeOptions, batch);
            this.rocksDB.put(this.confHandle, this.writeOptions, LAST_LOG_INDEX_KEY, ByteUtil.longToBytes(entries.getLast().getId().getIndex()));

            return count;
        }
        catch (RocksDBException e)
        {
            throw new RuntimeException(e);
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public void setAppliedLogIndex(long appliedLogIndex)
    {
        readLock.lock();
        try
        {
            this.rocksDB.put(this.confHandle, this.writeOptions, APPLIED_LOG_INDEX_KEY, ByteUtil.longToBytes(appliedLogIndex));
        }
        catch (RocksDBException e)
        {
            throw new RuntimeException(e);
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public long getAppliedLogIndex()
    {
        readLock.lock();
        checkState();
        try
        {
            byte[] bytes = this.rocksDB.get(this.confHandle, this.readOptions, APPLIED_LOG_INDEX_KEY);
            if (bytes != null) {
                return ByteUtil.bytesToLong(bytes);
            }
        }
        catch (RocksDBException e)
        {
            throw new RuntimeException(e);
        }
        finally {
            readLock.unlock();
        }
        return 0;
    }

    @Override
    public boolean truncatePrefix(long firstIndexKept)
    {
        writeLock.lock();
        try
        {
            long firstLogIndex = this.getFirstLogIndex();
            if (firstIndexKept < firstLogIndex) {
                return false;
            }

            for (long i = firstLogIndex; i <= firstIndexKept; i++)
            {
                this.rocksDB.delete(this.defaultHandle, this.writeOptions, ByteUtil.longToBytes(i));
            }

            this.rocksDB.put(this.confHandle, this.writeOptions, FIRST_LOG_INDEX_KEY, ByteUtil.longToBytes(firstLogIndex));
            return true;
        }
        catch (RocksDBException e)
        {
            throw new RuntimeException(e);
        }
        finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean truncateSuffix(long lastIndexKept)
    {
        writeLock.lock();
        try
        {
            long lastLogIndex = this.getLastLogIndex();
            if (lastIndexKept > lastLogIndex) {
                return false;
            }

            for (long i = lastIndexKept; i <= lastLogIndex; i++)
            {
                this.rocksDB.delete(this.defaultHandle, this.writeOptions, ByteUtil.longToBytes(i));
            }

            this.rocksDB.put(this.confHandle, this.writeOptions, LAST_LOG_INDEX_KEY, ByteUtil.longToBytes(lastIndexKept));
            return true;
        }
        catch (RocksDBException e)
        {
            throw new RuntimeException(e);
        }
        finally {
            writeLock.unlock();
        }
    }

    @Override
    public void shutdown() throws LifeCycleException
    {
        super.shutdown();
        this.rocksDB.close();

    }

    private void checkState()
    {
        Assert.notNull(this.rocksDB, "DB not initialized or destroyed");
    }
}
