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
package com.google.code.or;

import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogParser;
import com.google.code.or.binlog.BinlogParserListener;
import com.google.code.or.binlog.impl.ReplicationBasedBinlogParser;
import com.google.code.or.binlog.impl.parser.*;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.io.impl.DefaultSocketFactoryImpl;
import com.google.code.or.net.Packet;
import com.google.code.or.net.Transport;
import com.google.code.or.net.TransportException;
import com.google.code.or.net.impl.DefaultAuthenticatorImpl;
import com.google.code.or.net.impl.DefaultTransportImpl;
import com.google.code.or.net.impl.packet.ErrorPacket;
import com.google.code.or.net.impl.packet.command.ComBinlogDumpPacket;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 1. transport.connect
 * 2. dumpBinlog
 * 3. binlogParser.start
 *
 * @author Jingqi Xu
 * @author darnaut
 */
@Getter
@Setter
public class OpenReplicator {
    /**
     * MySQL 默认端口号
     */
    private int port = 3306;
    private String host;
    protected String user;
    private String password;
    private int serverId = 6789;
    private String binlogFileName;
    private long binlogPosition = 4;
    /**
     * TODO 什么文件的编码方式
     */
    private String encoding = "utf-8";
    /**
     * TODO ??
     */
    private int level1BufferSize = 1024 * 1024;
    /**
     * TODO ??
     */
    private int level2BufferSize = 8 * 1024 * 1024;
    /**
     * TODO ??
     */
    private int socketReceiveBufferSize = 512 * 1024;

    //
    private Transport transport;
    private BinlogParser binlogParser;
    private BinlogEventListener binlogEventListener;
    /**
     * flag 标示符
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 查看是否已经运行
     */
    public boolean isRunning() {
        return this.running.get();
    }

    /**
     * 1. transport.connect
     * 2. dumpBinlog
     * 3. binlogParser.start
     */
    public void start() throws Exception {
        // 状态标记为运行中
        if (!this.running.compareAndSet(false, true)) {
            return;
        }

        //
        if (this.transport == null) {
            this.transport = this.getDefaultTransport();
        }
        this.transport.connect(this.host, this.port);

        //
        this.dumpBinlog();

        //
        if (this.binlogParser == null) {
            this.binlogParser = this.getDefaultBinlogParser();
        }
        this.binlogParser.setEventListener(this.binlogEventListener);
        this.binlogParser.addParserListener(new BinlogParserListener.Adapter() {
            @Override
            public void onStop(BinlogParser parser) {
                stopQuietly(0, TimeUnit.MILLISECONDS);
            }
        });
        this.binlogParser.start();
    }

    public void stop(long timeout, TimeUnit unit) throws Exception {
        //
        if (!this.running.compareAndSet(true, false)) {
            return;
        }

        //
        this.transport.disconnect();
        this.binlogParser.stop(timeout, unit);
    }

    public void stopQuietly(long timeout, TimeUnit unit) {
        try {
            stop(timeout, unit);
        } catch (Exception e) {
            // NOP
        }
    }

    /**
     *
     */
    protected void dumpBinlog() throws Exception {
        //
        final ComBinlogDumpPacket command = new ComBinlogDumpPacket();
        command.setBinlogFlag(0);
        command.setServerId(this.serverId);
        command.setBinlogPosition(this.binlogPosition);
        command.setBinlogFileName(StringColumn.valueOf(this.binlogFileName.getBytes(this.encoding)));

        this.transport.getOutputStream().writePacket(command);
        this.transport.getOutputStream().flush();

        //
        final Packet packet = this.transport.getInputStream().readPacket();
        if (packet.getPacketBody()[0] == ErrorPacket.PACKET_MARKER) {
            final ErrorPacket error = ErrorPacket.valueOf(packet);
            throw new TransportException(error);
        }
    }

    protected Transport getDefaultTransport() {
        //
        final DefaultTransportImpl transport = new DefaultTransportImpl();
        transport.setLevel1BufferSize(this.level1BufferSize);
        transport.setLevel2BufferSize(this.level2BufferSize);

        //
        final DefaultAuthenticatorImpl authenticator = new DefaultAuthenticatorImpl();
        authenticator.setUser(this.user);
        authenticator.setPassword(this.password);
        authenticator.setEncoding(this.encoding);
        transport.setAuthenticator(authenticator);

        //
        final DefaultSocketFactoryImpl socketFactory = new DefaultSocketFactoryImpl();
        socketFactory.setKeepAlive(true);
        socketFactory.setTcpNoDelay(false);
        socketFactory.setReceiveBufferSize(this.socketReceiveBufferSize);
        transport.setSocketFactory(socketFactory);
        return transport;
    }

    protected ReplicationBasedBinlogParser getDefaultBinlogParser() {
        //
        final ReplicationBasedBinlogParser r = new ReplicationBasedBinlogParser();
        r.registerEventParser(new StopEventParser());
        r.registerEventParser(new RotateEventParser());
        r.registerEventParser(new IntvarEventParser());
        r.registerEventParser(new XidEventParser());
        r.registerEventParser(new RandEventParser());
        r.registerEventParser(new QueryEventParser());
        r.registerEventParser(new UserVarEventParser());
        r.registerEventParser(new IncidentEventParser());
        r.registerEventParser(new TableMapEventParser());
        r.registerEventParser(new WriteRowsEventParser());
        r.registerEventParser(new UpdateRowsEventParser());
        r.registerEventParser(new DeleteRowsEventParser());
        r.registerEventParser(new WriteRowsEventV2Parser());
        r.registerEventParser(new UpdateRowsEventV2Parser());
        r.registerEventParser(new DeleteRowsEventV2Parser());
        r.registerEventParser(new FormatDescriptionEventParser());
        r.registerEventParser(new GtidEventParser());

        //
        r.setTransport(this.transport);
        r.setBinlogFileName(this.binlogFileName);
        return r;
    }
}
