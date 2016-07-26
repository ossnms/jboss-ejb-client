/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
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

package org.jboss.ejb.client.test.independent;

import org.jboss.ejb.client.test.common.DummyServer;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Tests whether attempts to connect to unavailable servers interfere with other independent connection attempts.
 *
 * @author Joao Trindade
 */
public class IndependentConnectionsTestCase {

    private static final Logger logger = Logger.getLogger(IndependentConnectionsTestCase.class);

    private static final String ejbClientOneConfigResource = "connection-one-jboss-ejb-client.properties";
    private static final String ejbClientTwoConfigResource = "connection-two-jboss-ejb-client.properties";

    private Context initialContextOne;
    private Context initialContextTwo;

    private Context contextOne;
    private Context contextTwo;

    private ExecutorService executor;

    @Before
    public void setup() throws Exception {
        final Properties ejbClientOneProperties = loadPropertiesFrom(ejbClientOneConfigResource);
        final Properties ejbClientTwoProperties = loadPropertiesFrom(ejbClientTwoConfigResource);

        initialContextOne = (Context) new InitialContext(ejbClientOneProperties);
        initialContextTwo = (Context) new InitialContext(ejbClientTwoProperties);

        contextOne = null;
        contextTwo = null;

        executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread newThread = new Thread(runnable);
                newThread.setPriority(Thread.MAX_PRIORITY);
                return newThread;
            }
        });
    }

    @After
    public void tearDown() {
        executor.shutdown();
        safeClose(contextTwo);
        safeClose(contextOne);
    }

    /**
     * Tests that connecting to an available server succeeds, even if an independent connection
     * to another unreachable server is currently blocked.
     *
     * @throws Exception
     */
    @Test
    public void testConnectingWhileAnotherIndependentConnectionIsBlocked() throws Exception {
        DummyServer server = null;
        try {
            server = startServer();

            // operate on context two with a slight advance
            final CountDownLatch secondaryStarted = new CountDownLatch(1);
            Future<Context> contextTwoOper = executor.submit(new Callable<Context>() {
                @Override
                public Context call() throws Exception {
                    secondaryStarted.countDown();
                    // the following operation will block for the specified timeout
                    return contextTwo = (Context) initialContextTwo.lookup("ejb:");
                }
            });
            secondaryStarted.await();
            // wait one third of the timeout used for connection two
            pause(400);

            // now operate on connection one which should succeed without delay
            contextOne = (Context) initialContextOne.lookup("ejb:");

            // the connection with context two should still be blocked
            Assert.assertFalse("The lookup for ejb: should return with connection two still blocked", contextTwoOper.isDone());
            Assert.assertNotNull("Did not get a proper Context", contextOne);

        } finally {
            stopServer(server);
        }
    }

    /**
     * Tests that connecting to an available server succeeds, even if an independent connection
     * to another unreachable server is currently blocked.
     *
     * @throws Exception
     */
    @Test
    public void testConnectionTimeoutWhenAnotherIndependentConnectionIsBlocked() throws Exception {
        // operate on context two with a slight advance
        final CountDownLatch secondaryStarted = new CountDownLatch(1);
        Future<Context> contextTwoOper = executor.submit(new Callable<Context>() {
            @Override
            public Context call() throws Exception {
                secondaryStarted.countDown();
                // the following operation will block for the specified timeout
                return contextTwo = (Context) initialContextTwo.lookup("ejb:");
            }
        });
        secondaryStarted.await();
        // wait one third of the timeout used for connection two
        pause(400);

        // now operate on connection one with a shorter timeout and fail before connection two
        contextOne = (Context) initialContextOne.lookup("ejb:");

        // the connection with context two should still be blocked
        Assert.assertFalse("The lookup for ejb: should return with connection two still blocked", contextTwoOper.isDone());
    }

    /**
     * Starts and returns the server
     *
     * @return a started server
     * @throws java.io.IOException
     */
    private static DummyServer startServer() throws IOException {
        final DummyServer server = new DummyServer("localhost", 6999);
        server.start();
        return server;
    }

    private static void stopServer(DummyServer server) {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                // ignore
                logger.debug("Ignoring exception during server shutdown", e);
            }
        }
    }

    private void pause(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            logger.warn("An unexpected interrupt occurred");
        }
    }

    private static void safeClose(Object obj) {
        if (obj != null) {
            try {
                Method closeMethod = obj.getClass().getMethod("close");
                closeMethod.setAccessible(true);
                try {
                    closeMethod.invoke(obj);
                } catch (Throwable t) {
                    logger.warn("An exception was thrown while closing a " + obj.getClass().getSimpleName(), t);
                }
            } catch (NoSuchMethodException e) {
                logger.trace("Cannot close a non-closeable object of type " + obj.getClass().getSimpleName(), e);
            }
        }
    }

    private Properties loadPropertiesFrom(String resource) throws IOException {
        InputStream propertiesStream = null;
        try {
            propertiesStream = this.getClass().getClassLoader().getResourceAsStream(resource);
            Assert.assertNotNull("Could not find " + resource + " through classloader", propertiesStream);

            Properties properties = new Properties();
            properties.load(propertiesStream);

            return properties;
        } finally {
            if (propertiesStream != null) {
                propertiesStream.close();
            }
        }
    }
}
