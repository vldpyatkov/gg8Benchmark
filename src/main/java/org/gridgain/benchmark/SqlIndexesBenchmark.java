package org.gridgain.benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
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
@Fork(value = 1, jvmArgsPrepend = {"-Xmx4g"})
@Threads(32)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@SuppressWarnings({"WeakerAccess", "unused"})
public class SqlIndexesBenchmark {
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private static final ThreadLocal<Integer> GEN = ThreadLocal.withInitial(() -> COUNTER.getAndIncrement() * 20_000_000);

    private static final String STR10 = "qwertyuiop";
    private static final String STR100 = "qwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiop"
        + "qwertyuiopqwertyuiopqwertyuiopqwertyuiopqwertyuiop";
    public static final String CACHE_NAME = "cache";

    @Param({"0", "2", "4", "8", "10"})
    private int idxes;

    @Param({/*"INT",*/ "STR10", "STR100"})
    private String idxType;

    private IgniteCache<Integer, BinaryObject> cache;

    private Ignite ignite;

    /**
     * Initializes a schema and fills tables with data.
     */
    @Setup
    public void setUp() throws Exception {
        QueryEntity queryEntity = "INT".equals(idxType) ? queryForIntIndexes() : queryForStringIndexes();

        ArrayList<QueryIndex> indices = new ArrayList<>();

        switch (idxes) {
            case 10:
                indices.add(new QueryIndex("val9"));
            case 9:
                indices.add(new QueryIndex("val8"));
            case 8:
                indices.add(new QueryIndex("val7"));
            case 7:
                indices.add(new QueryIndex("val6"));
            case 6:
                indices.add(new QueryIndex("val5"));
            case 5:
                indices.add(new QueryIndex("val4"));
            case 4:
                indices.add(new QueryIndex("val3"));
            case 3:
                indices.add(new QueryIndex("val2"));
            case 2:
                indices.add(new QueryIndex("val1"));
            case 1:
                indices.add(new QueryIndex("val"));
        }

        queryEntity.setIndexes(indices);

        IgniteConfiguration ops = new IgniteConfiguration()
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder(true)))
            .setLocalHost("127.0.0.1")
            .setWorkDirectory(workDir().toAbsolutePath().toString())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(5368709120L)))
            .setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
                .setQueryEntities(Collections.singleton(queryEntity))
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL))
            .setPluginConfigurations(new GridGainConfiguration()
                .setLicenseUrl("C:\\ignite2\\gridgain\\ggprivate\\gridgain-license.xml"));

        ignite = Ignition.start(ops);

        ignite.cluster().active(true);

        cache = ignite.cache(CACHE_NAME);
    }

    private static QueryEntity queryForIntIndexes() {
        QueryEntity queryEntity = new QueryEntity();
        queryEntity.setTableName("test");
        queryEntity.setKeyType(Integer.class.getName());
        queryEntity.setValueType("test_val");
        queryEntity.addQueryField("val", Integer.class.getName(), null);
        queryEntity.addQueryField("val1", Integer.class.getName(), null);
        queryEntity.addQueryField("val2", Integer.class.getName(), null);
        queryEntity.addQueryField("val3", Integer.class.getName(), null);
        queryEntity.addQueryField("val4", Integer.class.getName(), null);
        queryEntity.addQueryField("val5", Integer.class.getName(), null);
        queryEntity.addQueryField("val6", Integer.class.getName(), null);
        queryEntity.addQueryField("val7", Integer.class.getName(), null);
        queryEntity.addQueryField("val8", Integer.class.getName(), null);
        queryEntity.addQueryField("val9", Integer.class.getName(), null);

        return queryEntity;
    }

    private static QueryEntity queryForStringIndexes() {
        QueryEntity queryEntity = new QueryEntity();
        queryEntity.setTableName("test");
        queryEntity.setKeyType(Integer.class.getName());
        queryEntity.setValueType("test_val");
        queryEntity.addQueryField("val", String.class.getName(), null);
        queryEntity.addQueryField("val1", String.class.getName(), null);
        queryEntity.addQueryField("val2", String.class.getName(), null);
        queryEntity.addQueryField("val3", String.class.getName(), null);
        queryEntity.addQueryField("val4", String.class.getName(), null);
        queryEntity.addQueryField("val5", String.class.getName(), null);
        queryEntity.addQueryField("val6", String.class.getName(), null);
        queryEntity.addQueryField("val7", String.class.getName(), null);
        queryEntity.addQueryField("val8", String.class.getName(), null);
        queryEntity.addQueryField("val9", String.class.getName(), null);

        return queryEntity;
    }

    @Benchmark
    public void put(Blackhole bh) {
        int val = ThreadLocalRandom.current().nextInt(0, 1_500_000);

        cache.put(nextId(), ignite.binary().builder("test_val")
            .setField("val", getVal(val))
            .setField("val1", getVal(val))
            .setField("val2", getVal(val))
            .setField("val3", getVal(val))
            .setField("val4", getVal(val))
            .setField("val5", getVal(val))
            .setField("val6", getVal(val))
            .setField("val7", getVal(val))
            .setField("val8", getVal(val))
            .setField("val9", getVal(val))
            .build()
        );
    }

    private int nextId() {
        int cur = GEN.get() + 1;
        GEN.set(cur);
        return cur;
    }

    private Object getVal(int val) {
        switch (idxType) {
            case "INT":
                return val;
            case "STR10": {
                String str = STR10 + val;

                return str.substring(str.length() - STR10.length());
            }
            case "STR100": {
                String str = STR100 + val;

                return str.substring(str.length() - STR100.length());
            }
            default:
                throw new IllegalArgumentException("Unsupported index type: " + idxType);
        }
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

    protected Path workDir() throws Exception {
        return Paths.get("D:", "Ignite2tmpDirPrefix" + ThreadLocalRandom.current().nextInt());
//        return Files.createTempDirectory("Ignite2tmpDirPrefix").toFile().toPath();
    }
}