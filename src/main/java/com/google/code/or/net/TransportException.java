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
package com.google.code.or.net;

import com.google.code.or.net.impl.packet.ErrorPacket;

import java.io.IOException;

/**
 *
 * @author Jingqi Xu
 */
public class TransportException extends IOException {

    private static final long serialVersionUID = 646149465892278906L;

    //
    private int errorCode;
    private String sqlState;
    private String errorMessage;

    /**
     *
     */
    public TransportException() {
    }

    public TransportException(String message) {
        super(message);
        this.errorMessage = message;
    }

    public TransportException(ErrorPacket ep) {
        super(ep.getErrorMessage().toString());
        this.errorCode = ep.getErrorCode();
        this.sqlState = ep.getSqlState().toString();
        this.errorMessage = ep.getErrorMessage().toString();
    }

    /**
     *
     */
    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getSqlState() {
        return sqlState;
    }

    public void setSqlState(String sqlState) {
        this.sqlState = sqlState;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
