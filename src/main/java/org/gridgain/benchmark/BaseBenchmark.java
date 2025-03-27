package org.gridgain.benchmark;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

public class BaseBenchmark {

    protected String licenseUrl() {
        return "C:\\ignite2\\gridgain\\ggprivate\\gridgain-license.xml";
    }

    protected long maxMemorySize() {
        return 5368709120L;
    }

    protected Path workDir() throws Exception {
        return Paths.get("D:", "Ignite2tmpDirPrefix" + ThreadLocalRandom.current().nextInt());
//        return Files.createTempDirectory("Ignite2tmpDirPrefix").toFile().toPath();
    }
}
