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
package com.google.code.or.binlog.impl.variable.status;

import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.common.util.MySQLConstants;
import com.google.code.or.common.util.ToStringBuilder;
import com.google.code.or.io.XInputStream;

import java.io.IOException;

/**
 *
 * @author Jingqi Xu
 */
public class QInvoker extends AbstractStatusVariable {
    //
    public static final int TYPE = MySQLConstants.Q_INVOKER;

    //
    private final StringColumn user;
    private final StringColumn host;

    /**
     *
     */
    public QInvoker(StringColumn user, StringColumn host) {
        super(TYPE);
        this.user = user;
        this.host = host;
    }

    /**
     *
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("user", user)
                .append("host", host).toString();
    }

    /**
     *
     */
    public StringColumn getUser() {
        return user;
    }

    public StringColumn getHost() {
        return host;
    }

    /**
     *
     */
    public static QInvoker valueOf(XInputStream tis) throws IOException {
        final int userLength = tis.readInt(1);
        final StringColumn user = tis.readFixedLengthString(userLength);
        final int hostLength = tis.readInt(1);
        final StringColumn host = tis.readFixedLengthString(hostLength);
        return new QInvoker(user, host);
    }
}
