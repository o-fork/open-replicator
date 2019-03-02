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
import com.google.code.or.binlog.impl.FileBasedBinlogParser;
import com.google.code.or.binlog.impl.parser.*;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jingqi Xu
 * @author darnaut
 */
@Getter
@Setter
public class OpenParser {
    //
    protected long stopPosition;
    protected long startPosition;
    protected String binlogFileName;
    protected String binlogFilePath;

    //
    protected BinlogParser binlogParser;
    protected BinlogEventListener binlogEventListener;
    protected final AtomicBoolean running = new AtomicBoolean(false);

    /**
     *
     */
    public boolean isRunning() {
        return this.running.get();
    }

    public void start() throws Exception {
        //
        if (!this.running.compareAndSet(false, true)) {
            return;
        }

        //
        if (this.binlogParser == null) {
            this.binlogParser = getDefaultBinlogParser();
        }

        this.binlogParser.setEventListener(this.binlogEventListener);
        this.binlogParser.start();
    }

    public void stop(long timeout, TimeUnit unit) throws Exception {
        //
        if (!this.running.compareAndSet(true, false)) {
            return;
        }

        //
        this.binlogParser.stop(timeout, unit);
    }

    /**
     *
     */
    protected FileBasedBinlogParser getDefaultBinlogParser() throws Exception {
        //
        final FileBasedBinlogParser r = new FileBasedBinlogParser();
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
        r.setStopPosition(this.stopPosition);
        r.setStartPosition(this.startPosition);
        r.setBinlogFileName(this.binlogFileName);
        r.setBinlogFilePath(this.binlogFilePath);
        return r;
    }
}
