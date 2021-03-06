/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.context;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.testng.annotations.Test;

/**
 * Tests DeepSparkContext instantiations.
 */
@Test(suiteName = "DeepSparkContextTest")
public class DeepSparkContextTest {

    public void testInstantiationBySparkContext() {
        DeepSparkContext sc = new CassandraDeepSparkContext(new SparkContext("local", "myapp1", new SparkConf()));

        sc.stop();
    }

    public void testInstantiationWithJar() {
        DeepSparkContext sc = new CassandraDeepSparkContext("local", "myapp1", "/tmp", "");
        sc.stop();
    }


    public void testInstantiationWithJars() {
        DeepSparkContext sc = new CassandraDeepSparkContext("local", "myapp1", "/tmp", new String[]{"", ""});
        sc.stop();
    }

    public void testInstantiationWithJarsAndEnv() {
        Map<String, String> env = new HashMap<>();
        DeepSparkContext sc = new CassandraDeepSparkContext("local", "myapp1", "/tmp", new String[]{"", ""}, env);
        sc.stop();
    }
}
