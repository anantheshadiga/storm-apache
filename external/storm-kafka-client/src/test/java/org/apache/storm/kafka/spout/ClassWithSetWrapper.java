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

import java.util.HashSet;
import java.util.Set;

class ClassWithSetWrapper {
//    ClassWithSet cws = new ClassWithSet(1);
//    ClassWithSet cws;

    /*public ClassWithSetWrapper(ClassWithSet cws) {
        System.out.println("ClassWithSetWrapper.ClassWithSetWrapper");
        this.cws = cws;
    }*/

    /*public void add(Integer i) {
        System.out.println("ClassWithSetWrapper.add");
        cws.add(i);
    }*/

    static class SC {

    }

    class ClassWithSet {
        private Set<Integer> si = new HashSet<>();
//        private Set<Integer> si;

        /*public ClassWithSet(ClassWithSetWrapper classWithSetWrapper, Set<Integer> si) {
            this.si = si;
        }*/

        /*public ClassWithSet(int bla) {
        }*/

        public ClassWithSet(SC bla) {
        }

        /*public ClassWithSet(Set<Integer> si) {
            System.out.println("ClassWithSet.ClassWithSet");
            this.si = si;
        }*/

        /*static {
            System.out.println("ClassWithSet.static initializer");
        }*/

        /*public ClassWithSet() {
            System.out.println("ClassWithSet.ClassWithSet");
        }*/

        public void add(Integer i) {
            System.out.println("ClassWithSet.add");
            si.add(i);
        }
    }

    private class ClassWithSet1 {
//        private Set<Integer> si = new HashSet<>();
        private Set<Integer> si;



        public ClassWithSet1() {
            System.out.println("ClassWithSet1.ClassWithSet");
        }

        public void add(Integer i) {
            System.out.println("ClassWithSet1.add");
            si.add(i);
        }
    }
}
