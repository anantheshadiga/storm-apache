/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Set;

import mockit.Deencapsulation;
import mockit.Injectable;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Tested;
import mockit.Verifications;
import mockit.integration.junit4.JMockit;

@RunWith(JMockit.class)
public class SomeTest {
    @Tested(availableDuringSetup = false)
    private ClassWithSet classWithSet;
//    @Capturing Set<Integer> si;

    private class MockUpClassWithSet extends MockUp<ClassWithSet> {
//        private Set<Integer> si = new HashSet<>();

        @Mock
        void $init() {
            System.out.println("MockUpClassWithSet.$init");
        }

        @Mock
        void $clinit() {
            System.out.println("MockUpClassWithSet.$clinit");
            // Do nothing here (usually).
        }

        @Mock
        void add(Invocation invocation, Integer i) {
            System.out.println("MockUpClassWithSet.add");
            invocation.proceed(i);
            System.out.println("after proceed");
            Object invokedInstance = invocation.getInvokedInstance();
        }
    }


    @Test
//    public void testCWS(@Injectable final Set<Integer> si) throws Exception {
    public void testCWSAssert() throws Exception {
        classWithSet.add(3);

        /*new Verifications() {{
            Integer val;
            si.add(anyInt); times = 1;
            si.add(val = withCapture()); times = 1;
//            Integer ni = si.iterator().next();
            Assert.assertEquals((Integer)3, val);
        }};*/

        Set<Integer> si1 = Deencapsulation.getField(classWithSet, "si");
        Assert.assertEquals(1, si1.size());
        Assert.assertEquals((Integer) 3, si1.iterator().next());
    }

    @Test
    public void testCWSVerify(@Injectable final Set<Integer> si) throws Exception {
        classWithSet.add(3);

        new Verifications() {{
            Integer val;
            si.add(anyInt); times = 1;
            si.add(val = withCapture()); times = 1;
//            Integer ni = si.iterator().next();
            Assert.assertEquals((Integer)3, val);
        }};
    }

    @Test
    public void testCWSMock(@Injectable final Set<Integer> si) throws Exception {

        new MockUpClassWithSet();

        classWithSet.add(3);

        new Verifications() {{
            Integer val;
            si.add(anyInt);
            times = 1;
            si.add(val = withCapture());
            times = 1;
//            Integer ni = si.iterator().next();
            Assert.assertEquals((Integer) 3, val);
        }};
    }

    @Test
    public void testCWSMockBasic(@Injectable final Set<Integer> si) throws Exception {
        new MockUpClassWithSet();
        classWithSet.add(3);
    }
}
