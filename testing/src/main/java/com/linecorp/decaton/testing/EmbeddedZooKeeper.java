/*
 * Copyright 2020 LINE Corporation
 *
 * LINE Corporation licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.linecorp.decaton.testing;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;

import org.apache.kafka.common.utils.Utils;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * Starts embedded ZooKeeper server in a same process on random port
 */
public class EmbeddedZooKeeper implements AutoCloseable {
    private final int port;
    private final ServerCnxnFactory cnxnFactory;
    private final File snapshotDir;
    private final File logDir;

    public EmbeddedZooKeeper() {
        try {
            snapshotDir = Files.createTempDirectory("zookeeper-snapshot").toFile();
            logDir = Files.createTempDirectory("zookeeper-logs").toFile();
            ZooKeeperServer zkServer = new ZooKeeperServer(snapshotDir,
                                                           logDir,
                                                           ZooKeeperServer.DEFAULT_TICK_TIME);

            InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 0);
            // disable max client cnxns by passing 0
            cnxnFactory = ServerCnxnFactory.createFactory(addr, 0);
            cnxnFactory.startup(zkServer);

            port = zkServer.getClientPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String zkConnectAsString() {
        return "127.0.0.1:" + port;
    }

    @Override
    public void close() {
        cnxnFactory.shutdown();

        safeDelete(snapshotDir);
        safeDelete(logDir);
    }

    private static void safeDelete(File file) {
        try {
            Utils.delete(file);
        } catch (IOException e) {
            System.err.printf("Failed to delete %s\n", file);
        }
    }
}
