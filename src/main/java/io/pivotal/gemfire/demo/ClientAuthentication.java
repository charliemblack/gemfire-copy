/*
 * Copyright 2017 Charlie Black
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.pivotal.gemfire.demo;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.AuthenticationFailedException;

import java.util.Properties;

public class ClientAuthentication implements AuthInitialize {
    public static final String USER_NAME = "security-username";
    public static final String PASSWORD = "security-password";
    private LogWriter logger;

    public static AuthInitialize create() {
        return new ClientAuthentication();
    }

    public void init(LogWriter systemLogger, LogWriter securityLogger)
            throws AuthenticationFailedException {
        this.logger = securityLogger;
    }

    public Properties getCredentials(Properties securityProps, DistributedMember server, boolean isPeer) throws AuthenticationFailedException {
        Properties credentials = new Properties();
        String userName = securityProps.getProperty(USER_NAME);
        if (userName == null) {
            throw new AuthenticationFailedException("ClientAuthentication: user name property [" + USER_NAME + "] not set.");
        }
        credentials.setProperty(USER_NAME, userName);
        String passwd = securityProps.getProperty(PASSWORD);
        if (passwd == null) {
            throw new AuthenticationFailedException("ClientAuthentication: password property [" + PASSWORD + "] not set.");
        }
        credentials.setProperty(PASSWORD, passwd);
        logger.info("ClientAuthentication: successfully obtained credentials for user " + userName);
        return credentials;
    }

    public void close() {
    }
}
