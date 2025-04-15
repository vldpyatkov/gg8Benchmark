package org.gridgain.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.gridgain.grid.configuration.GridGainConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@Fork(value = 1/*, jvmArgsPrepend = {"-Xmx4g"}*/)
@Threads(16)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class SqlIndexesBenchmark extends BaseBenchmark {
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private static final ThreadLocal<Integer> GEN = ThreadLocal.withInitial(() -> COUNTER.getAndIncrement() * 20_000_000);

    public static final String CACHE_NAME = "cache";

    @Param({"0", "10"})
    private int idxes;

    @Param({"100"})
    private int fieldLength;

    @Param({"uniquePrefix", "uniquePostfix"})
    private String fieldValueGeneration;

    private IgniteCache<Integer, BinaryObject> cache;

    private Ignite ignite;

    /**
     * Initializes a schema and fills tables with data.
     */
    @Setup
    public void setUp() throws Exception {
        QueryEntity queryEntity = queryForStringIndexes();

        ArrayList<QueryIndex> indices = new ArrayList<>();

        if (idxes > 10) {
            throw new IllegalStateException("Unexpected value of idxes: " + idxes);
        }

        for (int i = 1; i <= idxes; i++) {
            indices.add(new QueryIndex("field" + i));
        }

        queryEntity.setIndexes(indices);

        IgniteConfiguration ops = new IgniteConfiguration()
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder(true)))
            .setLocalHost("127.0.0.1")
            .setWorkDirectory(workDir().toAbsolutePath().toString())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setWalSegmentSize(1024 * 1024 * 1024)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(maxMemorySize())))
            .setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
                .setQueryEntities(Collections.singleton(queryEntity))
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL))
            .setPluginConfigurations(new GridGainConfiguration()
                .setLicenseUrl(licenseUrl()));

        ignite = Ignition.start(ops);

        ignite.cluster().active(true);

        cache = ignite.cache(CACHE_NAME);
    }

    private static QueryEntity queryForStringIndexes() {
        QueryEntity queryEntity = new QueryEntity();
        queryEntity.setTableName("test");
        queryEntity.setKeyType(Integer.class.getName());
        queryEntity.setValueType("ycsb_key");
        queryEntity.addQueryField("field1", String.class.getName(), null);
        queryEntity.addQueryField("field2", String.class.getName(), null);
        queryEntity.addQueryField("field3", String.class.getName(), null);
        queryEntity.addQueryField("field4", String.class.getName(), null);
        queryEntity.addQueryField("field5", String.class.getName(), null);
        queryEntity.addQueryField("field6", String.class.getName(), null);
        queryEntity.addQueryField("field7", String.class.getName(), null);
        queryEntity.addQueryField("field8", String.class.getName(), null);
        queryEntity.addQueryField("field9", String.class.getName(), null);
        queryEntity.addQueryField("field10", String.class.getName(), null);

        return queryEntity;
    }

    private BinaryObject valueTuple(int id) {
        String formattedString = String.format("%" + (fieldValueGeneration.equals("uniquePrefix") ? '-' : '0') + fieldLength + "d", id);

        String fieldVal = formattedString.length() > fieldLength ? formattedString.substring(0, fieldLength) : formattedString;

        return ignite.binary().builder("ycsb_key")
            .setField("field1", fieldVal)
            .setField("field2", fieldVal)
            .setField("field3", fieldVal)
            .setField("field4", fieldVal)
            .setField("field5", fieldVal)
            .setField("field6", fieldVal)
            .setField("field7", fieldVal)
            .setField("field8", fieldVal)
            .setField("field9", fieldVal)
            .setField("field10", fieldVal)
            .build();
    }

    @Benchmark
    public void put(Blackhole bh) {
        int id = nextId();

        cache.put(id, valueTuple(id));
    }

    private int nextId() {
        int cur = GEN.get() + 1;
        GEN.set(cur);
        return cur;
    }

    /**
     * Stops the cluster.
     *
     * @throws Exception In case of any error.
     */
    @TearDown
    public final void nodeTearDown() throws Exception {
        ignite.close();
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(".*" + SqlIndexesBenchmark.class.getSimpleName() + ".*")
            .build();

        new Runner(opt).run();
    }
}