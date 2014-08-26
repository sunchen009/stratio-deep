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

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

import com.google.common.io.Resources;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.stratio.deep.config.CassandraConfigFactory;
import com.stratio.deep.config.DeepJobConfig;
import com.stratio.deep.config.ICassandraDeepJobConfig;
import com.stratio.deep.embedded.CassandraServer;
import com.stratio.deep.exception.DeepIOException;
import com.stratio.deep.functions.AbstractSerializableFunction;
import com.stratio.deep.testentity.Cql3CollectionsTestEntity;
import com.stratio.deep.utils.Constants;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.Function1;
import scala.reflect.ClassTag$;

import static com.stratio.deep.utils.Utils.quote;
import static org.testng.Assert.*;

/**
 * Integration tests for entity RDDs where cells contain Cassandra's collections.
 */
@Test(suiteName = "cassandraRddTests", dependsOnGroups = "ScalaCassandraEntityRDDTest",
        groups = "CassandraCollectionsEntityTest")
public class CassandraCollectionsEntityTest extends CassandraRDDTest<Cql3CollectionsTestEntity> {

    private static Logger logger = Logger.getLogger(CassandraCollectionsEntityTest.class);

    @BeforeClass
    protected void initServerAndRDD() throws IOException, URISyntaxException, ConfigurationException,
            InterruptedException {
        super.initServerAndRDD();

        loadCollectionsData();
    }

    static void loadCollectionsData() {
        truncateCf(KEYSPACE_NAME, CQL3_COLLECTION_COLUMN_FAMILY);

        URL cql3TestData = Resources.getResource("cql3_collections_test_data.csv");

        Batch batch = QueryBuilder.batch();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(
                cql3TestData.toURI()))))) {
            String line;

            int idx = 0;
            final int emailsStartPos = 3;

            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");

                Set<String> emails = new HashSet<String>();
                List<String> phones = new ArrayList<String>();
                Map<UUID, Integer> uuid2id = new HashMap<>();

                for (int k = emailsStartPos; k < emailsStartPos + 2; k++) {
                    emails.add(fields[k]);
                }

                for (int k = emailsStartPos + 2; k < emailsStartPos + 4; k++) {
                    phones.add(fields[k]);
                }

                UUID uuid = UUID.fromString(fields[emailsStartPos + 4]);
                Integer id = Integer.parseInt(fields[0]);

                uuid2id.put(uuid, id);

                Insert stmt = QueryBuilder.insertInto(CQL3_COLLECTION_COLUMN_FAMILY).values(
                        new String[]{"id", "first_name", "last_name", "emails", "phones", "uuid2id"},
                        new Object[]{Integer.parseInt(fields[0]), fields[1], fields[2], emails, phones, uuid2id});

                batch.add(stmt);
                ++idx;
            }

            logger.debug("idx: " + idx);
        } catch (Exception e) {
            logger.error("Error", e);
        }


        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect(quote(KEYSPACE_NAME));
        session.execute(batch);
    }

    @Override
    protected void checkComputedData(Cql3CollectionsTestEntity[] entities) {

        boolean found = false;

        assertEquals(entities.length, 500);

        for (Cql3CollectionsTestEntity e : entities) {
            if (e.getId().equals(470)) {
                Assert.assertEquals(e.getFirstName(), "Amalda");
                Assert.assertEquals(e.getLastName(), "Banks");
                assertNotNull(e.getEmails());
                Assert.assertEquals(e.getEmails().size(), 2);
                Assert.assertEquals(e.getPhones().size(), 2);
                Assert.assertEquals(e.getUuid2id().size(), 1);
                Assert.assertEquals(e.getUuid2id().get(UUID.fromString("AE47FBFD-A086-47C2-8C73-77D8A8E99F35")),
                        Integer.valueOf(470));

                Iterator<String> emails = e.getEmails().iterator();
                Iterator<String> phones = e.getPhones().iterator();

                assertEquals(emails.next(), "AmaldaBanks@teleworm.us");
                assertEquals(emails.next(), "MarcioColungaPichardo@dayrep.com");

                assertEquals(phones.next(), "801-527-1039");
                assertEquals(phones.next(), "925-348-9339");
                found = true;
                break;
            }
        }

        if (!found) {
            fail();
        }

    }

    @Override
    protected void checkSimpleTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();

        String command = "select count(*) from " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY
                + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 500);

        command = "select * from " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY
                + " WHERE \"id\" = 351;";

        rs = session.execute(command);
        Row row = rs.one();

        String firstName = row.getString("first_name");
        String lastName = row.getString("last_name");
        Set<String> emails = row.getSet("emails", String.class);
        List<String> phones = row.getList("phones", String.class);
        Map<UUID, Integer> uuid2id = row.getMap("uuid2id", UUID.class, Integer.class);

        assertEquals(firstName, "Gustava");
        assertEquals(lastName, "Palerma");
        assertNotNull(emails);
        assertEquals(emails.size(), 2);

        assertNotNull(phones);
        assertEquals(phones.size(), 2);

        assertNotNull(uuid2id);
        assertEquals(uuid2id.size(), 1);
        assertEquals(uuid2id.get(UUID.fromString("BAB7F03E-0D9F-4466-BD8A-5F7373802610")).intValue(), 351);

        session.close();

    }

    @Override
    protected RDD<Cql3CollectionsTestEntity> initRDD() {
        return context.createRDD(getReadConfig());
    }

    @Override
    protected DeepJobConfig<Cql3CollectionsTestEntity> initReadConfig() {
        DeepJobConfig<Cql3CollectionsTestEntity> config = CassandraConfigFactory.create(Cql3CollectionsTestEntity.class)
                .host(Constants.DEFAULT_CASSANDRA_HOST).rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT).bisectFactor(testBisectFactor)
                .cqlPort(CassandraServer.CASSANDRA_CQL_PORT).keyspace(KEYSPACE_NAME)
				        .pageSize(DEFAULT_PAGE_SIZE).columnFamily(CQL3_COLLECTION_COLUMN_FAMILY);

        return config.initialize();
    }

    @Override
    protected DeepJobConfig<Cql3CollectionsTestEntity> initWriteConfig() {
        DeepJobConfig<Cql3CollectionsTestEntity> writeConfig = CassandraConfigFactory.createWriteConfig
				        (Cql3CollectionsTestEntity.class)
                .host(Constants.DEFAULT_CASSANDRA_HOST)
                .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
                .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
                .keyspace(OUTPUT_KEYSPACE_NAME)
                .columnFamily(OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY)
                .batchSize(2)
                .createTableOnWrite(Boolean.TRUE);
        return writeConfig.initialize();
    }

    @Override
    public void testSaveToCassandra() {

        fail("revieW");
       /*  TODO REVIEW
      Function1<Cql3CollectionsTestEntity, Cql3CollectionsTestEntity> mappingFunc =
                new TestEntityAbstractSerializableFunction();

        RDD<Cql3CollectionsTestEntity> mappedRDD =
                getRDD().map(mappingFunc, ClassTag$.MODULE$.<Cql3CollectionsTestEntity>apply
                        (Cql3CollectionsTestEntity.class));

        try {
            executeCustomCQL("DROP TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY);
        } catch (Exception e) {
        }

        assertTrue(mappedRDD.count() > 0);

        ICassandraDeepJobConfig<Cql3CollectionsTestEntity> writeConfig = getWriteConfig();
        writeConfig.createTableOnWrite(Boolean.FALSE);

        try {
            CassandraRDD.saveRDDToCassandra(mappedRDD, writeConfig);

            fail();
        } catch (DeepIOException e) {
            // ok
            logger.info("Correctly catched DeepIOException: " + e.getMessage());
            writeConfig.createTableOnWrite(Boolean.TRUE);
        }

        CassandraRDD.saveRDDToCassandra(mappedRDD, writeConfig);

        checkOutputTestData();*/
    }

    protected void checkOutputTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();

        String command = "select count(*) from " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY
                + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 500);

        command = "select * from " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY
                + " WHERE \"id\" = 351;";

        rs = session.execute(command);
        Row row = rs.one();

        String firstName = row.getString("first_name");
        String lastName = row.getString("last_name");
        Set<String> emails = row.getSet("emails", String.class);
        List<String> phones = row.getList("phones", String.class);
        Map<UUID, Integer> uuid2id = row.getMap("uuid2id", UUID.class, Integer.class);

        assertEquals(firstName, "Gustava_out");
        assertEquals(lastName, "Palerma_out");
        assertNotNull(emails);
        assertEquals(emails.size(), 3);
        assertTrue(emails.contains("klv@email.com"));

        assertNotNull(phones);
        assertEquals(phones.size(), 3);
        assertTrue(phones.contains("111-111-1111112"));

        assertNotNull(uuid2id);
        assertEquals(uuid2id.size(), 1);
        assertEquals(uuid2id.get(UUID.fromString("BAB7F03E-0D9F-4466-BD8A-5F7373802610")).intValue() - 10, 351);

        session.close();
    }

    @Override
    public void testSimpleSaveToCassandra() {
        DeepJobConfig<Cql3CollectionsTestEntity> writeConfig = getWriteConfig();
        writeConfig.createTableOnWrite(Boolean.FALSE);

        try {
            executeCustomCQL("DROP TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY);
        } catch (Exception e) {
        }

        try {
            CassandraRDD.saveRDDToCassandra(getRDD(), writeConfig);

            fail();
        } catch (Exception e) {
            // ok
            logger.info("Correctly catched Exception: " + e.getMessage());
            writeConfig.createTableOnWrite(Boolean.TRUE);
        }

        CassandraRDD.saveRDDToCassandra(getRDD(), writeConfig);

        checkSimpleTestData();
    }

    @Override
    public void testCql3SaveToCassandra() {

        try {
            executeCustomCQL("DROP TABLE " + OUTPUT_KEYSPACE_NAME + "." + OUTPUT_CQL3_COLLECTION_COLUMN_FAMILY);
        } catch (Exception e) {
        }

        DeepJobConfig<Cql3CollectionsTestEntity> writeConfig = getWriteConfig();

        CassandraRDD.cql3SaveRDDToCassandra(getRDD(), writeConfig);
        checkSimpleTestData();

    }

    private static class TestEntityAbstractSerializableFunction extends
            AbstractSerializableFunction<Cql3CollectionsTestEntity, Cql3CollectionsTestEntity> {

        private static final long serialVersionUID = -1555102599662015841L;

        @Override
        public Cql3CollectionsTestEntity apply(Cql3CollectionsTestEntity e) {
            Cql3CollectionsTestEntity out = new Cql3CollectionsTestEntity();

            out.setId(e.getId());
            out.setFirstName(e.getFirstName() + "_out");
            out.setLastName(e.getLastName() + "_out");

            Set<String> emails = e.getEmails();
            emails.add("klv@email.com");

            out.setEmails(emails);

            List<String> phones = e.getPhones();
            phones.add("111-111-1111112");

            out.setPhones(phones);

            Map<UUID, Integer> uuid2id = e.getUuid2id();
            for (Map.Entry<UUID, Integer> entry : uuid2id.entrySet()) {
                entry.setValue(entry.getValue() + 10);
            }

            out.setUuid2id(uuid2id);

            return out;
        }
    }
}
