/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller;

import org.apache.kafka.common.requests.ApiError;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.BROKER0;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.CONFIGS;
//import static org.apache.kafka.controller.ConfigurationControlManagerTest.MYTOPIC;
import static org.apache.kafka.controller.ConfigurationControlManagerTest.entry;
import static org.junit.Assert.assertEquals;

public class QuorumControllerTest {
    private static final Logger log =
        LoggerFactory.getLogger(QuorumControllerTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    @Test
    public void testCreateAndClose() throws Throwable {
        try (LocalQuorumsTestEnv env = new LocalQuorumsTestEnv(1, __ -> { })) {
        }
    }

    @Test
    public void testWriteOperations() throws Throwable {
        try (LocalQuorumsTestEnv env = new LocalQuorumsTestEnv(1,
            builder -> builder.setConfigDefs(CONFIGS))) {
            assertEquals(Collections.singletonMap(BROKER0, ApiError.NONE),
                env.activeController().incrementalAlterConfigs(Collections.singletonMap(
                    BROKER0, Collections.singletonMap("baz", entry(SET, "123"))), true).get());
            assertEquals(Collections.singletonMap(BROKER0,
                    new ResultOrError<>(Collections.emptyMap())),
                env.activeController().describeConfigs(Collections.singletonMap(
                    BROKER0, Collections.emptyList())).get());
            assertEquals(Collections.singletonMap(BROKER0, ApiError.NONE),
                env.activeController().incrementalAlterConfigs(Collections.singletonMap(
                    BROKER0, Collections.singletonMap("baz", entry(SET, "123"))), false).get());
            assertEquals(Collections.singletonMap(BROKER0, new ResultOrError<>(Collections.
                    singletonMap("baz", "123"))),
                env.activeController().describeConfigs(Collections.singletonMap(
                    BROKER0, Collections.emptyList())).get());
        }
    }
}
