package org.gridgain.benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

public class BaseBenchmark {

    protected String licenseUrl() {
//        return "C:\\ignite2\\gridgain\\ggprivate\\gridgain-license.xml";
        return "/root/pvd/gg8Benchmark/gridgain-license.xml";
    }

    protected long maxMemorySize() {
        return 21474836480L; // 20 GB
    }

    protected Path workDir() throws Exception {
//        return Paths.get("D:", "Ignite2tmpDirPrefix" + ThreadLocalRandom.current().nextInt());
        return Files.createTempDirectory("Ignite2tmpDirPrefix").toFile().toPath();
    }
}
