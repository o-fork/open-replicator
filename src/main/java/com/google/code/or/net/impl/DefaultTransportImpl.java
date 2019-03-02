/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.code.or.net.impl;

import com.google.code.or.common.util.IOUtils;
import com.google.code.or.io.SocketFactory;
import com.google.code.or.io.util.ActiveBufferedInputStream;
import com.google.code.or.net.Packet;
import com.google.code.or.net.TransportException;
import com.google.code.or.net.TransportInputStream;
import com.google.code.or.net.TransportOutputStream;
import com.google.code.or.net.impl.packet.ErrorPacket;
import com.google.code.or.net.impl.packet.GreetingPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jingqi Xu
 */
public class DefaultTransportImpl extends AbstractTransport {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTransportImpl.class);

    //
    protected Socket socket;
    protected TransportInputStream transportInputStream;
    protected TransportOutputStream transportOutputStream;
    protected SocketFactory socketFactory;
    protected int level1BufferSize = 1024 * 1024;
    protected int level2BufferSize = 8 * 1024 * 1024;
    protected final AtomicBoolean connected = new AtomicBoolean(false);

    /**
     *
     */
    @Override
    public boolean isConnected() {
        return this.connected.get();
    }

    /**
     * 1. 创建到 MySQL 的 Socket 链接
     *
     * @param host MySQL 主机名
     * @param port MySQL 端口号
     */
    @Override
    public void connect(String host, int port) throws Exception {
        //
        if (!this.connected.compareAndSet(false, true)) {
            return;
        }

        //
        if (isVerbose() && LOGGER.isInfoEnabled()) {
            LOGGER.info("connecting to host: {}, port: {}", host, port);
        }

        //
        this.socket = this.socketFactory.create(host, port);

        this.transportOutputStream = new TransportOutputStreamImpl(this.socket.getOutputStream());

        if (this.level2BufferSize <= 0) {
            this.transportInputStream = new TransportInputStreamImpl(this.socket.getInputStream(), this.level1BufferSize);
        } else {
            this.transportInputStream = new TransportInputStreamImpl(new ActiveBufferedInputStream(this.socket.getInputStream(), this.level2BufferSize), this.level1BufferSize);
        }

        //
        final Packet packet = this.transportInputStream.readPacket();
        if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
            final ErrorPacket error = ErrorPacket.valueOf(packet);
            LOGGER.info("failed to connect to host: {}, port: {}, error", new Object[]{host, port, error});
            throw new TransportException(error);
        } else {
            //
            final GreetingPacket greeting = GreetingPacket.valueOf(packet);
            this.context.setServerHost(host);
            this.context.setServerPort(port);
            this.context.setServerStatus(greeting.getServerStatus());
            this.context.setServerVersion(greeting.getServerVersion().toString());
            this.context.setServerCollation(greeting.getServerCollation());
            this.context.setServerCapabilities(greeting.getServerCapabilities());
            this.context.setThreadId(greeting.getThreadId());
            this.context.setProtocolVersion(greeting.getProtocolVersion());
            this.context.setScramble(greeting.getScramble1().toString() + greeting.getScramble2().toString());

            //
            if (isVerbose() && LOGGER.isInfoEnabled()) {
                LOGGER.info("connected to host: {}, port: {}, context: {}", new Object[]{host, port, this.context});
            }
        }

        //
        this.authenticator.login(this);
    }

    @Override
    public void disconnect() throws Exception {
        //
        if (!this.connected.compareAndSet(true, false)) {
            return;
        }

        //
        IOUtils.closeQuietly(this.transportInputStream);
        IOUtils.closeQuietly(this.transportOutputStream);
        IOUtils.closeQuietly(this.socket);

        //
        if (isVerbose() && LOGGER.isInfoEnabled()) {
            LOGGER.info("disconnected from {}:{}", this.context.getServerHost(), this.context.getServerPort());
        }
    }

    @Override
    public TransportInputStream getInputStream() {
        return this.transportInputStream;
    }

    @Override
    public TransportOutputStream getOutputStream() {
        return this.transportOutputStream;
    }

    /**
     *
     */
    public int getLevel1BufferSize() {
        return level1BufferSize;
    }

    public void setLevel1BufferSize(int size) {
        this.level1BufferSize = size;
    }

    public int getLevel2BufferSize() {
        return level2BufferSize;
    }

    public void setLevel2BufferSize(int size) {
        this.level2BufferSize = size;
    }


    public SocketFactory getSocketFactory() {
        return socketFactory;
    }

    public void setSocketFactory(SocketFactory factory) {
        this.socketFactory = factory;
    }
}
