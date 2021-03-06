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
package com.google.code.or.net.impl.packet;

import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.common.util.ToStringBuilder;
import com.google.code.or.io.util.XDeserializer;
import com.google.code.or.io.util.XSerializer;
import com.google.code.or.net.Packet;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author Jingqi Xu
 */
public class ResultSetRowPacket extends AbstractPacket {
    //
    private static final long serialVersionUID = 698187140476020984L;

    //
    private List<StringColumn> columns;

    /**
     *
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("columns", columns).toString();
    }

    /**
     *
     */
    public byte[] getPacketBody() {
        final XSerializer s = new XSerializer(1024);
        for (StringColumn column : this.columns) {
            s.writeLengthCodedString(column);
        }
        return s.toByteArray();
    }

    /**
     *
     */
    public List<StringColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<StringColumn> columns) {
        this.columns = columns;
    }

    /**
     *
     */
    public static ResultSetRowPacket valueOf(Packet packet) throws IOException {
        final XDeserializer d = new XDeserializer(packet.getPacketBody());
        final ResultSetRowPacket r = new ResultSetRowPacket();
        r.length = packet.getLength();
        r.sequence = packet.getSequence();
        r.setColumns(new LinkedList<StringColumn>());
        while (d.available() > 0) {
            r.getColumns().add(d.readLengthCodedString());
        }
        return r;
    }
}
