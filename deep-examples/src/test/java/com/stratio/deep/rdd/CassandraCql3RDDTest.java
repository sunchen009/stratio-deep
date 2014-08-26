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

package com.stratio.deep.rdd;

import java.util.List;

import com.stratio.deep.config.ExtractorConfig;
import scala.reflect.ClassTag$;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.stratio.deep.config.CassandraConfigFactory;

import com.stratio.deep.embedded.CassandraServer;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.exception.DeepIndexNotFoundException;
import com.stratio.deep.exception.DeepNoSuchFieldException;
import com.stratio.deep.functions.AbstractSerializableFunction;

import com.stratio.deep.testentity.Cql3TestEntity;
import com.stratio.deep.utils.Constants;
import org.apache.spark.rdd.RDD;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Function1;


import static org.testng.Assert.*;

/**
 * Created by luca on 03/02/14.
 */
@Test(suiteName = "cassandraRddTests", groups = {"CassandraCql3RDDTest"}, dependsOnGroups = {"CassandraEntityRDDTest"})
public class CassandraCql3RDDTest extends CassandraRDDTest<Cql3TestEntity> {

    private static class TestEntityAbstractSerializableFunction extends
            AbstractSerializableFunction<Cql3TestEntity, Cql3TestEntity> {
        private static final long serialVersionUID = 6678218192781434399L;

        @Override
        public Cql3TestEntity apply(Cql3TestEntity e) {
            return new Cql3TestEntity(e.getName(), e.getPassword(), e.getColor(), e.getGender(), e.getFood(),
                    e.getAnimal(), e.getLucene());
        }
    }

    @Override
    protected void checkComputedData(Cql3TestEntity[] entities) {

        boolean found = false;

        assertEquals(entities.length, cql3TestDataSize);

        for (Cql3TestEntity e : entities) {
            if (e.getName().equals("pepito_3") && e.getAge().equals(-2) && e.getGender().equals("male")
                    && e.getAnimal().equals("monkey")) {
                assertNull(e.getColor());
                assertNull(e.getLucene());
                Assert.assertEquals(e.getFood(), "donuts");
                Assert.assertEquals(e.getPassword(), "abc");
                found = true;
                break;
            }
        }

        if (!found) {
            fail();
        }

    }

    @Test
    public void testAdditionalFilters() {
        try {
            CassandraConfigFactory
                    .create(Cql3TestEntity.class)
                    .host(Constants.DEFAULT_CASSANDRA_HOST)
                    .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
                    .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
                    .keyspace(KEYSPACE_NAME)
                    .columnFamily(CQL3_COLUMN_FAMILY)
                    .filterByField("notExistentField", "val")
                    .initialize();

            fail();
        } catch (DeepNoSuchFieldException e) {
            // OK
        }

        try {
            CassandraConfigFactory
                    .create(Cql3TestEntity.class)
                    .host(Constants.DEFAULT_CASSANDRA_HOST)
                    .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
                    .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
                    .keyspace(KEYSPACE_NAME)
                    .columnFamily(CQL3_COLUMN_FAMILY)
                    .filterByField("lucene", "val")
                    .initialize();

            fail();
        } catch (DeepIndexNotFoundException e) {
            // OK
        }

        Cql3TestEntity[] entities = (Cql3TestEntity[]) rdd.collect();

        int allElements = entities.length;
        assertTrue(allElements > 1);

        ExtractorConfig<Cql3TestEntity> config = new ExtractorConfig<>(Cql3TestEntity.class);

        RDD<Cql3TestEntity> otherRDD = context.createRDD(config);

        entities = (Cql3TestEntity[]) otherRDD.collect();

        assertEquals(entities.length, 1);
        Assert.assertEquals(entities[0].getFood(), "donuts");
        Assert.assertEquals(entities[0].getName(), "pepito_3");
        Assert.assertEquals(entities[0].getGender(), "male");
        Assert.assertEquals(entities[0].getAge(), Integer.valueOf(-2));
        Assert.assertEquals(entities[0].getAnimal(), "monkey");

        config = CassandraConfigFactory
                .create(Cql3TestEntity.class)
                .host(Constants.DEFAULT_CASSANDRA_HOST)
                .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
                .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
                .keyspace(KEYSPACE_NAME)
                .columnFamily(CQL3_COLUMN_FAMILY)
                .filterByField("food", "chips")
                .initialize();

        otherRDD = context.createRDD(config);

        entities = (Cql3TestEntity[]) otherRDD.collect();
        assertEquals(entities.length, allElements - 1);

        /*
        otherRDD.removeFilterOnField("food");
        entities = (Cql3TestEntity[]) otherRDD.collect();
        assertEquals(entities.length, allElements);
        */
    }


    protected void checkOutputTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();

        String command = "select count(*) from " + OUTPUT_KEYSPACE_NAME + "." + CQL3_ENTITY_OUTPUT_COLUMN_FAMILY + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 4);

        command = "SELECT * from " + OUTPUT_KEYSPACE_NAME + "." + CQL3_ENTITY_OUTPUT_COLUMN_FAMILY + ";";

        rs = session.execute(command);
        for (Row r : rs) {
            assertEquals(r.getInt("age"), 15);
        }
        session.close();
    }

    @Override
    protected void checkSimpleTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();

        String command = "select count(*) from " + OUTPUT_KEYSPACE_NAME + "." + CQL3_ENTITY_OUTPUT_COLUMN_FAMILY + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), cql3TestDataSize);

        command = "select * from " + OUTPUT_KEYSPACE_NAME + "." + CQL3_ENTITY_OUTPUT_COLUMN_FAMILY
                + " WHERE name = 'pepito_1' and gender = 'male' and age = 0  and animal = 'monkey';";
        rs = session.execute(command);

        List<Row> rows = rs.all();

        assertNotNull(rows);
        assertEquals(rows.size(), 1);

        Row r = rows.get(0);

        assertEquals(r.getString("password"), "xyz");

        session.close();
    }

    @Override
    protected RDD<Cql3TestEntity> initRDD() {
        assertNotNull(context);
        return context.createRDD(getReadConfig());
    }

    @Override
    protected DeepJobConfig<Cql3TestEntity> initReadConfig() {
        return CassandraConfigFactory.create(Cql3TestEntity.class).host(Constants.DEFAULT_CASSANDRA_HOST).bisectFactor(testBisectFactor)
                .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
				        .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
				        .pageSize(DEFAULT_PAGE_SIZE).keyspace(KEYSPACE_NAME).columnFamily(CQL3_COLUMN_FAMILY)
                .initialize();
    }

    @Override
    protected com.stratio.deep.config.DeepJobConfig<Cql3TestEntity> initWriteConfig() {

        return CassandraConfigFactory.createWriteConfig(Cql3TestEntity.class).host(Constants.DEFAULT_CASSANDRA_HOST)
                .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT).keyspace(OUTPUT_KEYSPACE_NAME)
                .cqlPort(CassandraServer.CASSANDRA_CQL_PORT).columnFamily(CQL3_ENTITY_OUTPUT_COLUMN_FAMILY)
                .createTableOnWrite(Boolean.TRUE).initialize();
    }

    @Override
    public void testSaveToCassandra() {
        Function1<Cql3TestEntity, Cql3TestEntity> mappingFunc = new TestEntityAbstractSerializableFunction();
//        ClassTag$.MODULE$.<T>apply(config
//                .getEntityClass())
        RDD<Cql3TestEntity> mappedRDD = getRDD().map(mappingFunc, ClassTag$.MODULE$.<Cql3TestEntity>apply(Cql3TestEntity.class));

        try {
            executeCustomCQL("DROP TABLE " + OUTPUT_KEYSPACE_NAME + "." + CQL3_ENTITY_OUTPUT_COLUMN_FAMILY);
        } catch (Exception e) {
        }

        assertTrue(mappedRDD.count() > 0);

        DeepJobConfig<Cql3TestEntity> writeConfig = getWriteConfig();
        writeConfig.createTableOnWrite(Boolean.FALSE);

        try {
            CassandraRDD.saveRDDToCassandra(mappedRDD, writeConfig);
            fail();
        } catch (DeepIOException e) {
            // ok
            writeConfig.createTableOnWrite(Boolean.TRUE);
        }

        CassandraRDD.saveRDDToCassandra(mappedRDD, writeConfig);
        checkOutputTestData();
    }

    @Override
    public void testSimpleSaveToCassandra() {
        try {
            executeCustomCQL("DROP TABLE " + OUTPUT_KEYSPACE_NAME + "." + CQL3_ENTITY_OUTPUT_COLUMN_FAMILY);
        } catch (Exception e) {
        }

        DeepJobConfig<Cql3TestEntity> writeConfig = getWriteConfig();
        writeConfig.createTableOnWrite(Boolean.FALSE);

        try {
            CassandraRDD.saveRDDToCassandra(getRDD(), writeConfig);
            fail();
        } catch (DeepIOException e) {
            // ok
            writeConfig.createTableOnWrite(Boolean.TRUE);
        }

        CassandraRDD.saveRDDToCassandra(getRDD(), writeConfig);

        checkSimpleTestData();

    }

    @Override
    public void testCql3SaveToCassandra() {
        try {
            executeCustomCQL("DROP TABLE " + OUTPUT_KEYSPACE_NAME + "." + CQL3_ENTITY_OUTPUT_COLUMN_FAMILY);
        } catch (Exception e) {
        }

        DeepJobConfig<Cql3TestEntity> writeConfig = getWriteConfig();

        CassandraRDD.cql3SaveRDDToCassandra(getRDD(), writeConfig);
        checkSimpleTestData();
    }
}
