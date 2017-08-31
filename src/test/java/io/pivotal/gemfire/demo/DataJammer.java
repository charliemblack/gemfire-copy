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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer;
import org.junit.Test;

import javax.annotation.Resource;
import java.util.Date;

public class DataJammer {


    @Test
    public void insertAWholeBunch() throws JsonProcessingException {
        ClientCacheFactory factory = new ClientCacheFactory();
        factory.set("security-client-auth-init", "io.pivotal.gemfire.demo.ClientAuthentication.create");
        factory.set("security-username", "super-user");
        factory.set("security-password", "1234567");
        factory.addPoolLocator("localhost", 10334);
        factory.setPdxSerializer(new ReflectionBasedAutoSerializer("demo.geode.greenplum.*"));
        ClientCache clientCache = factory.create();
        Region testRegion = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("Region1");
        int counter = 1;
        ObjectMapper objectMapper = new ObjectMapper();
        while (true) {
            SampleData sampleData = new SampleData(counter++, new Date(), "some text" + counter++, counter % 2 == 0);
            String json = objectMapper.writeValueAsString(sampleData);
            testRegion.put(counter, JSONFormatter.fromJSON(json));
        }
    }
}
