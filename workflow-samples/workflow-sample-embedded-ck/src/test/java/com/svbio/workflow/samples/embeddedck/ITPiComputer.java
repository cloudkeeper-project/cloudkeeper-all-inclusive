package com.svbio.workflow.samples.embeddedck;

import com.svbio.cloudkeeper.model.api.util.RecursiveDeleteVisitor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ITPiComputer {
    private static final String DB_CONNECTION = "jdbc:h2:mem:" + ITPiComputer.class.getSimpleName();

    private Path tempDir;

    @BeforeClass
    public void startup() throws IOException {
        tempDir = Files.createTempDirectory(getClass().getName());
    }

    @AfterClass
    public void tearDown() throws IOException, InterruptedException {
        Files.walkFileTree(tempDir, RecursiveDeleteVisitor.getInstance());
    }

    @Test
    public void test() throws IOException, SQLException {
        // The H2 documentation says: "By default, closing the last connection to a database closes the database. For an
        // in-memory database, this means the content is lost." We therefore make sure a connection is open during the
        // entire test.
        try (Connection ignored = DriverManager.getConnection(DB_CONNECTION)) {
            Map<String, Object> configMap = new LinkedHashMap<>();
            configMap.put("com.svbio.workflow.database.javax.persistence.jdbc.url", DB_CONNECTION);
            configMap.put("com.svbio.workflow.localexecutor.workspacebasepath", tempDir.toString());
            configMap.put("com.svbio.workflow.drmaa.tmpdir", tempDir.toString());

            // Note that specialized static configuration resides in application.conf.
            Config config = ConfigFactory.parseMap(configMap).withFallback(ConfigFactory.load());
            String pi = PiComputer.computePi(config);
            Assert.assertEquals(pi, "3.141592653");
        }
    }
}
