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
package com.google.code.or.net.impl.packet.command;

import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.common.util.MySQLConstants;
import com.google.code.or.io.util.XSerializer;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Jingqi Xu
 */
@Getter
@Setter
@ToString
public class ComBinlogDumpPacket extends AbstractCommandPacket {

    private static final long serialVersionUID = 449639496684376511L;

    //
    private long binlogPosition;
    private int binlogFlag;
    private long serverId;
    private StringColumn binlogFileName;

    /**
     *
     */
    public ComBinlogDumpPacket() {
        super(MySQLConstants.COM_BINLOG_DUMP);
    }

    /**
     *
     */
    @Override
    public byte[] getPacketBody() {
        final XSerializer ps = new XSerializer();

        ps.writeInt(this.command, 1);
        ps.writeLong(this.binlogPosition, 4);
        ps.writeInt(this.binlogFlag, 2);
        ps.writeLong(this.serverId, 4);
        ps.writeFixedLengthString(this.binlogFileName);

        return ps.toByteArray();
    }

}
