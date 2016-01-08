/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2013, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.ejb.client.remoting;

import java.io.Closeable;
import java.io.IOException;
import java.security.Principal;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSession;
import javax.security.auth.callback.CallbackHandler;

import org.jboss.ejb.client.EJBClientConfiguration;
import org.jboss.logging.Logger;
import org.jboss.remoting3.Attachments;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.Endpoint;
import org.jboss.remoting3.HandleableCloseable;
import org.jboss.remoting3.security.UserInfo;
import org.xnio.IoFuture;
import org.xnio.OptionMap;

/**
 * A pool which creates and hands out remoting {@link Connection}s and maintains a reference count to close the connections handed
 * out, when the count reaches zero.
 *
 * @author Jaikiran Pai
 * Courtesy: Remote naming project
 */
class ConnectionPool {
    private static final Logger logger = Logger.getLogger(ConnectionPool.class);

    static final ConnectionPool INSTANCE = new ConnectionPool();

    static final Thread SHUTDOWN_TASK = new Thread(new ShutdownTask(INSTANCE));

    static {
        SecurityActions.addShutdownHook(SHUTDOWN_TASK);
    }

    private final ConcurrentMap<CacheKey, PooledConnection> cache = new ConcurrentHashMap<CacheKey, PooledConnection>();

    private ConnectionPool() {

    }

    Connection getConnection(final Endpoint clientEndpoint, final String protocol, final String host, final int port, final EJBClientConfiguration.CommonConnectionCreationConfiguration connectionConfiguration) throws IOException {
        final CacheKey key = new CacheKey(clientEndpoint, connectionConfiguration.getCallbackHandler(), connectionConfiguration.getConnectionCreationOptions(), host, port, protocol);
        PooledConnection pooledConnection;
        boolean connected = false;

        // ensure there is a cache entry representing a request with the current parameters
        synchronized (this) {
            pooledConnection = cache.get(key);
            if (pooledConnection == null) {
                pooledConnection = new PooledConnection(clientEndpoint, protocol, host, port, connectionConfiguration.getConnectionCreationOptions(), connectionConfiguration.getCallbackHandler(), key, new CacheEntryRemovalHandler(key));
                cache.put(key, pooledConnection);
            }
            pooledConnection.incRef();
        }

        // wait for the connection to be established
        try {
            pooledConnection.waitConnected(connectionConfiguration.getConnectionTimeout(), TimeUnit.MILLISECONDS);
            connected = true;
        } finally {
            if (!connected) {
                release(key, false);
            }
        }

        return pooledConnection;
    }

    void release(final CacheKey connectionHash, final boolean async) {
        final PooledConnection pooledConnection = cache.get(connectionHash);
        if (pooledConnection == null) {
            return;
        }
        synchronized (this) {
            if (pooledConnection.decRef() > 0) {
                return;
            }
            cache.remove(connectionHash);
        }
        pooledConnection.destroy(async);
    }

    private void shutdown() {
        Collection<PooledConnection> pooledConnections;
        synchronized (this) {
            pooledConnections = cache.values();
            cache.clear();
        }
        for (PooledConnection pooledConnection : pooledConnections) {
            pooledConnection.destroy(false);
        }

        if(Thread.currentThread().getId() != SHUTDOWN_TASK.getId())
            SecurityActions.removeShutdownHook(SHUTDOWN_TASK);
    }

    /**
     * The key to the pooled connection
     */
    private static final class CacheKey {
        final Endpoint endpoint;
        final String host;
        final int port;
        final String protocol;
        final OptionMap connectOptions;
        final CallbackHandler callbackHandler;

        private CacheKey(final Endpoint endpoint, final CallbackHandler callbackHandler, final OptionMap connectOptions, final String host, final int port, final String protocol) {
            this.endpoint = endpoint;
            this.callbackHandler = callbackHandler;
            this.connectOptions = connectOptions;
            this.host = host;
            this.port = port;
            this.protocol = protocol;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey) o;

            if (port != cacheKey.port) return false;
            if (callbackHandler != null ? !callbackHandler.equals(cacheKey.callbackHandler) : cacheKey.callbackHandler != null)
                return false;
            if (connectOptions != null ? !connectOptions.equals(cacheKey.connectOptions) : cacheKey.connectOptions != null)
                return false;
            if (endpoint != null ? !endpoint.equals(cacheKey.endpoint) : cacheKey.endpoint != null) return false;
            if (host != null ? !host.equals(cacheKey.host) : cacheKey.host != null) return false;
            if (protocol != null ? !protocol.equals(cacheKey.protocol) : cacheKey.protocol != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = endpoint != null ? endpoint.hashCode() : 0;
            result = 31 * result + (host != null ? host.hashCode() : 0);
            result = 31 * result + port;
            result = 31 * result + (connectOptions != null ? connectOptions.hashCode() : 0);
            result = 31 * result + (callbackHandler != null ? callbackHandler.hashCode() : 0);
            result = 31 * result + (protocol != null ? protocol.hashCode() : 0);
            return result;
        }
    }


    private static void safeClose(Closeable closable) {
        try {
            closable.close();
        } catch (Throwable t) {
            logger.debug("Failed to close " + closable, t);
        }
    }

    /**
     * The pooled connection
     */
    private final class PooledConnection implements Connection {
        private int referenceCount = 0;
        private final CacheKey cacheKey;
        private Connection underlyingConnection;

        private final CacheEntryRemovalHandler cacheEntryRemovalHandler;
        private final Endpoint clientEndpoint;
        private final String protocol;
        private final String host;
        private final int port;
        private final OptionMap connectionCreationOptions;
        private final CallbackHandler callbackHandler;

        private boolean destroyed = false;

        PooledConnection(Endpoint clientEndpoint, String protocol, String host, int port, OptionMap connectionCreationOptions, CallbackHandler callbackHandler, CacheKey key, CacheEntryRemovalHandler cacheEntryRemovalHandler) {
            this.cacheKey = key;
            this.cacheEntryRemovalHandler = cacheEntryRemovalHandler;
            this.clientEndpoint = clientEndpoint;
            this.protocol = protocol;
            this.host = host;
            this.port = port;
            this.connectionCreationOptions = connectionCreationOptions;
            this.callbackHandler = callbackHandler;
        }

        int incRef() {
            return ++referenceCount;
        }

        int decRef() {
            return --referenceCount;
        }

        synchronized void waitConnected(long connectionTimeout, TimeUnit timeUnit) throws IOException {
            if (destroyed) {
                throw new IOException("Shutdown in progress. No new connections allowed.");
            }
            if (underlyingConnection == null) {
                IoFuture<Connection> futureConnection = NetworkUtil.connect(clientEndpoint, protocol, host, port, null, connectionCreationOptions, callbackHandler, null);
                // wait for the connection to be established
                underlyingConnection = IoFutureHelper.get(futureConnection, connectionTimeout, timeUnit);
                // We don't want to hold stale connection(s), so add a close handler which removes the entry
                // from the cache when the connection is closed
                underlyingConnection.addCloseHandler(cacheEntryRemovalHandler);
            }
        }

        synchronized void destroy(boolean async) {
            if (!destroyed) {
                destroyed = true;
                if (underlyingConnection != null) {
                    if (async) {
                        underlyingConnection.closeAsync();
                    } else {
                        safeClose(underlyingConnection);
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            release(this.cacheKey, false);
        }

        @Override
        public void closeAsync() {
            release(this.cacheKey, true);
        }

        @Override
        public void awaitClosed() throws InterruptedException {
            this.underlyingConnection.awaitClosed();
        }

        @Override
        public void awaitClosedUninterruptibly() {
            this.underlyingConnection.awaitClosedUninterruptibly();
        }

        @Override
        public Key addCloseHandler(CloseHandler<? super Connection> closeHandler) {
            return this.underlyingConnection.addCloseHandler(closeHandler);
        }

        @Override
        public Collection<Principal> getPrincipals() {
            return this.underlyingConnection.getPrincipals();
        }

        @Override
        public UserInfo getUserInfo() {
            return this.underlyingConnection.getUserInfo();
        }

        @Override
        public IoFuture<Channel> openChannel(String s, OptionMap options) {
            return this.underlyingConnection.openChannel(s, options);
        }

        @Override
        public String getRemoteEndpointName() {
            return this.underlyingConnection.getRemoteEndpointName();
        }

        @Override
        public Endpoint getEndpoint() {
            return this.underlyingConnection.getEndpoint();
        }

        @Override
        public Attachments getAttachments() {
            return this.underlyingConnection.getAttachments();
        }

        @Override
        public SSLSession getSslSession() {
            return this.underlyingConnection.getSslSession();
        }
    }

    /**
     * A {@link Runtime#addShutdownHook(Thread) shutdown task} which {@link org.jboss.ejb.client.remoting.ConnectionPool#shutdown() shuts down}
     * the connection pool
     */
    private static final class ShutdownTask implements Runnable {

        private final ConnectionPool pool;

        ShutdownTask(final ConnectionPool pool) {
            this.pool = pool;
        }

        @Override
        public void run() {
            // close all pooled connections
            pool.shutdown();
        }
    }

    /**
     * A {@link CloseHandler} which removes a entry from the {@link ConnectionPool}.
     */
    private class CacheEntryRemovalHandler implements CloseHandler<HandleableCloseable> {

        private final CacheKey key;

        CacheEntryRemovalHandler(final CacheKey key) {
            this.key = key;
        }

        @Override
        public void handleClose(HandleableCloseable closable, IOException e) {
            cache.remove(this.key);
        }
    }

}
