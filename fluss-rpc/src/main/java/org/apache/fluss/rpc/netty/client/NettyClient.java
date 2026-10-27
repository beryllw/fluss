/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.rpc.netty.client;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.DisconnectException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.rpc.netty.NettyMetrics;
import org.apache.fluss.rpc.netty.NettyUtils;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.security.auth.AuthenticationFactory;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to
 * implement the user-facing reader and writer.
 */
@ThreadSafe
public final class NettyClient implements RpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

    /** Netty's Bootstrap. */
    private final Bootstrap bootstrap;

    /** Netty's event group. */
    private final EventLoopGroup eventGroup;

    /**
     * Managed connections to Netty servers. The key is the server uid (e.g., "cs-2", "ts-3"), the
     * value is the result of creating the connection, which may still be connecting.
     */
    private final Map<String, CompletableFuture<ServerConnection>> connections;

    /** Metric groups for client. */
    private final ClientMetricGroup clientMetricGroup;

    private final Supplier<ClientAuthenticator> authenticatorSupplier;

    private volatile boolean isClosed = false;

    public NettyClient(Configuration conf, ClientMetricGroup clientMetricGroup) {
        this.connections = new ConcurrentHashMap<>();

        // build bootstrap
        this.eventGroup =
                NettyUtils.newEventLoopGroup(
                        conf.getInt(ConfigOptions.NETTY_CLIENT_NUM_NETWORK_THREADS),
                        "fluss-netty-client");
        int connectTimeoutMs = (int) conf.get(ConfigOptions.CLIENT_CONNECT_TIMEOUT).toMillis();
        int connectionMaxIdle =
                (int) conf.get(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME).getSeconds();
        boolean preferHeap =
                conf.getBoolean(ConfigOptions.NETTY_CLIENT_ALLOCATOR_HEAP_BUFFER_FIRST);
        PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
        this.bootstrap =
                new Bootstrap()
                        .group(eventGroup)
                        .channel(NettyUtils.getClientSocketChannelClass(eventGroup))
                        .option(ChannelOption.ALLOCATOR, allocator)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .option(ChannelOption.SO_KEEPALIVE, true)
                        .handler(new ClientChannelInitializer(connectionMaxIdle, preferHeap));
        this.clientMetricGroup = clientMetricGroup;
        this.authenticatorSupplier = AuthenticationFactory.loadClientAuthenticatorSupplier(conf);
        NettyMetrics.registerNettyMetrics(clientMetricGroup, allocator);
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send
     * to that node.
     *
     * @param node The server node to check
     * @return True if we are ready to send to the given node.
     */
    @Override
    public boolean connect(ServerNode node) {
        checkArgument(!isClosed, "Netty client is closed.");
        return isReady(getOrCreateConnection(node));
    }

    /**
     * Disconnects the connection to the given server node, if there is one. Any inflight/pending
     * requests for this connection will receive disconnections.
     *
     * @param serverUid The uid of the server node
     * @return A future that completes when the connection is fully closed
     */
    @Override
    public CompletableFuture<Void> disconnect(String serverUid) {
        LOG.debug("Disconnecting from server {}.", serverUid);
        checkArgument(!isClosed, "Netty client is closed.");
        CompletableFuture<ServerConnection> closing = new CompletableFuture<>();
        while (true) {
            CompletableFuture<ServerConnection> connection = connections.get(serverUid);
            if (connection == null) {
                return FutureUtils.completedVoidFuture();
            }
            // Reserve the slot until construction and cleanup finish before allowing a replacement.
            if (connections.replace(serverUid, connection, closing)) {
                return closeConnection(connection)
                        .whenComplete(
                                (ignored, error) -> {
                                    connections.remove(serverUid, closing);
                                    closing.completeExceptionally(
                                            new NetworkException(
                                                    new DisconnectException(
                                                            "Disconnected from node "
                                                                    + serverUid)));
                                });
            }
        }
    }

    /**
     * Check if we are currently ready to send another request to the given server but don't attempt
     * to connect if we aren't.
     *
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(String serverUid) {
        checkArgument(!isClosed, "Netty client is closed.");
        return isReady(connections.get(serverUid));
    }

    /** Send an RPC request to the given server and return a future for the response. */
    @Override
    public CompletableFuture<ApiMessage> sendRequest(
            ServerNode node, ApiKeys apiKey, ApiMessage request) {
        checkArgument(!isClosed, "Netty client is closed.");
        CompletableFuture<ServerConnection> connection = getOrCreateConnection(node);
        if (connection.isDone() && !connection.isCompletedExceptionally()) {
            return connection.getNow(null).send(apiKey, request);
        }
        return connection.thenCompose(con -> con.send(apiKey, request));
    }

    @Override
    public void close() throws Exception {
        try {
            isClosed = true;
            final List<CompletableFuture<Void>> shutdownFutures = new ArrayList<>();
            for (Map.Entry<String, CompletableFuture<ServerConnection>> conn :
                    connections.entrySet()) {
                if (connections.remove(conn.getKey(), conn.getValue())) {
                    shutdownFutures.add(closeConnection(conn.getValue()));
                }
            }
            shutdownFutures.add(NettyUtils.shutdownGroup(eventGroup));
            CompletableFuture.allOf(shutdownFutures.toArray(new CompletableFuture<?>[0]))
                    .get(10, TimeUnit.SECONDS);
            LOG.info("Netty client was shutdown successfully.");
        } catch (Exception e) {
            LOG.warn("Netty client shutdown failed: ", e);
        }
    }

    private CompletableFuture<ServerConnection> getOrCreateConnection(ServerNode node) {
        String serverId = node.uid();
        CompletableFuture<ServerConnection> existing = connections.get(serverId);
        if (existing != null) {
            return existing;
        }

        CompletableFuture<ServerConnection> connectionFuture = new CompletableFuture<>();
        existing = connections.putIfAbsent(serverId, connectionFuture);
        if (existing != null) {
            return existing;
        }

        try {
            checkArgument(!isClosed, "Netty client is closed.");
            LOG.debug("Creating connection to server {}.", node);
            ServerConnection connection =
                    new ServerConnection(
                            bootstrap,
                            node,
                            clientMetricGroup,
                            authenticatorSupplier.get(),
                            (con, ignore) -> connections.remove(serverId, connectionFuture));
            // A disconnect or shutdown may have removed this attempt during construction.
            if (isClosed || connections.get(serverId) != connectionFuture) {
                connection.close();
            }
            connectionFuture.complete(connection);
        } catch (Throwable t) {
            connections.remove(serverId, connectionFuture);
            connectionFuture.completeExceptionally(t);
            ExceptionUtils.rethrowIfFatalError(t);
        }
        return connectionFuture;
    }

    private static boolean isReady(CompletableFuture<ServerConnection> connection) {
        return connection != null
                && connection.isDone()
                && !connection.isCompletedExceptionally()
                && connection.getNow(null).isReady();
    }

    private static CompletableFuture<Void> closeConnection(
            CompletableFuture<ServerConnection> connection) {
        return connection
                .handle(
                        (con, error) ->
                                error == null ? con.close() : FutureUtils.completedVoidFuture())
                .thenCompose(future -> future);
    }

    @VisibleForTesting
    Map<String, CompletableFuture<ServerConnection>> connections() {
        return connections;
    }

    @VisibleForTesting
    Bootstrap bootstrap() {
        return bootstrap;
    }
}
