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

package com.linecorp.decaton.benchmark;

import java.io.IOException;
import java.net.ServerSocket;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import com.linecorp.decaton.benchmark.Execution.Stage;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Remote interface used for communication between forked execution and benchmark controller.
 *
 * {@link Stage} changes are notified through {@link BenchmarkRmi#pollStage()} rather than
 * RMI callback directly which requires benchmark controller to be concurrent aware.
 */
@Slf4j
class BenchmarkRmi implements AutoCloseable {
    private static final String REMOTE_NAME = "BenchmarkRmi";

    private final BlockingQueue<Stage> stageQueue = new LinkedBlockingQueue<>();
    private RemoteCallback callback;
    /**
     * Port of the RMI registry
     */
    @Getter
    @Accessors(fluent = true)
    private int port;

    interface RemoteCallback extends Remote {
        /**
         * Called when {@link Stage} of the execution has changed
         * @param stage updated {@link Stage}
         */
        void onStage(Stage stage) throws RemoteException;

        /**
         * Called when the execution has done
         * @param serializedResult JSON-serialized result of the benchmark execution
         */
        void onResult(String serializedResult) throws RemoteException;
    }

    /**
     * {@link RMIServerSocketFactory} impl which stores actual bound port
     */
    private static class SocketFactory implements RMIServerSocketFactory {
        private int port;

        @Override
        public ServerSocket createServerSocket(int port) throws IOException {
            ServerSocket sock = RMISocketFactory.getDefaultSocketFactory().createServerSocket(port);
            this.port = sock.getLocalPort();
            return sock;
        }
    }

    /**
     * Poll {@link Stage} changes which is emitted from forked execution
     * @return {@link Stage} of the execution
     */
    Stage pollStage() {
        try {
            return stageQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Start RMI server then accept {@link RemoteCallback} invocation from forked execution
     * @return future of the JSON-serialized {@link BenchmarkResult}
     */
    CompletableFuture<String> start() {
        CompletableFuture<String> resultFuture = new CompletableFuture<>();
        callback = new RemoteCallback() {
            @Override
            public void onStage(Stage stage) throws RemoteException {
                try {
                    stageQueue.put(stage);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onResult(String serializedResult) throws RemoteException {
                resultFuture.complete(serializedResult);
            }
        };

        SocketFactory socketFactory = new SocketFactory();
        try {
            LocateRegistry.createRegistry(0, RMISocketFactory.getSocketFactory(), socketFactory)
                          .bind(REMOTE_NAME, UnicastRemoteObject.exportObject(callback, 0));
            port = socketFactory.port;
            log.debug("Started registry on port: {}", port);

            return resultFuture;
        } catch (RemoteException | AlreadyBoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (callback != null) {
            try {
                UnicastRemoteObject.unexportObject(callback, true);
            } catch (Exception e) {
                log.warn("Failed to unexport remote object", e);
            }
        }
        if (port != 0) {
            try {
                LocateRegistry.getRegistry(port).unbind(REMOTE_NAME);
            } catch (Exception e) {
                log.warn("Failed to unbind registry", e);
            }
        }
    }

    /**
     * Lookup {@link RemoteCallback} instance from RMI registry
     * @param port port of the registry
     * @return remote {@link RemoteCallback} instance
     */
    static RemoteCallback lookup(int port) {
        try {
            return (RemoteCallback) LocateRegistry.getRegistry(port).lookup(REMOTE_NAME);
        } catch (IOException | NotBoundException e) {
            throw new RuntimeException(e);
        }
    }
}
