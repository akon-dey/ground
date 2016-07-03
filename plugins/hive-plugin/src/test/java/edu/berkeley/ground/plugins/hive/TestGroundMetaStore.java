/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.plugins.hive;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.FileFormatProxy;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.hbase.HBaseConnection;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.annotations.VisibleForTesting;

import edu.berkeley.ground.api.models.cassandra.CassandraEdgeVersionFactory;
import edu.berkeley.ground.api.models.cassandra.CassandraGraphVersionFactory;
import edu.berkeley.ground.api.models.cassandra.CassandraNodeVersionFactory;

public class TestGroundMetaStore {
    private GroundStore groundStore = null;
    static final String GRAPHFACTORY_CLASS = "ground.graph.factory";

    static final String NODEFACTORY_CLASS = "ground.node.factory";

    static final String EDGEFACTORY_CLASS = "ground.edge.factory";

    private static final String DB1 = "testobjectstoredb1";
    private static final String DB2 = "testobjectstoredb2";
    private static final String TABLE1 = "testobjectstoretable1";
    private static final String KEY1 = "testobjectstorekey1";
    private static final String KEY2 = "testobjectstorekey2";
    private static final String OWNER = "testobjectstoreowner";
    private static final String USER1 = "testobjectstoreuser1";
    private static final String ROLE1 = "testobjectstorerole1";
    private static final String ROLE2 = "testobjectstorerole2";

    public static class MockPartitionExpressionProxy implements PartitionExpressionProxy {
        @Override
        public String convertExprToFilter(byte[] expr) throws MetaException {
            return null;
        }

        @Override
        public boolean filterPartitionsByExpr(List<String> partColumnNames, List<PrimitiveTypeInfo> partColumnTypeInfos,
                byte[] expr, String defaultPartitionName, List<String> partitionNames) throws MetaException {
            return false;
        }

        @Override
        public FileMetadataExprType getMetadataType(String inputFormat) {
            return null;
        }

        @Override
        public SearchArgument createSarg(byte[] expr) {
            return null;
        }

        @Override
        public FileFormatProxy getFileFormatProxy(FileMetadataExprType type) {
            return null;
        }
    }

    @Before
    public void setUp() throws Exception {
        HiveConf conf = new HiveConf();
        conf.setVar(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS, MockPartitionExpressionProxy.class.getName());
        HiveConf.setVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "test_connection");
        conf.set(GRAPHFACTORY_CLASS, CassandraGraphVersionFactory.class.getName());
        conf.set(NODEFACTORY_CLASS, CassandraNodeVersionFactory.class.getName());
        conf.set(EDGEFACTORY_CLASS, CassandraEdgeVersionFactory.class.getName());
        GroundReadWrite.setConf(conf);
        groundStore = new GroundStore();
        groundStore.setConf(conf);
        dropAllStoreObjects(groundStore);
    }

    @After
    public void tearDown() {
    }

    /**
     * Test database operations
     */
    @Test
    public void testDatabaseOps() throws MetaException, InvalidObjectException, NoSuchObjectException {
        Database db1 = new Database(DB1, "description", "locationurl", new HashMap<String, String>());
        Database db2 = new Database(DB2, "description", "locationurl", new HashMap<String, String>());
        groundStore.createDatabase(db1);
        groundStore.createDatabase(db2);

        List<String> databases = groundStore.getAllDatabases();
        Assert.assertEquals(2, databases.size());
        Assert.assertEquals(DB1, databases.get(0));
        Assert.assertEquals(DB2, databases.get(1));

        groundStore.dropDatabase(DB1);
        databases = groundStore.getAllDatabases();
        Assert.assertEquals(1, databases.size());
        Assert.assertEquals(DB2, databases.get(0));

        groundStore.dropDatabase(DB2);
    }

    /**
     * Test table operations
     */
    @Test
    public void testTableOps()
            throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
        Database db1 = new Database(DB1, "description", "locationurl", null);
        groundStore.createDatabase(db1);
        StorageDescriptor sd = new StorageDescriptor(null, "location", null, null, false, 0,
                new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("EXTERNAL", "false");
        Table tbl1 = new Table(TABLE1, DB1, "owner", 1, 2, 3, sd, null, params, "viewOriginalText", "viewExpandedText",
                "MANAGED_TABLE");
        groundStore.createTable(tbl1);

        List<String> tables = groundStore.getAllTables(DB1);
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(TABLE1, tables.get(0));

        Table newTbl1 = new Table("new" + TABLE1, DB1, "owner", 1, 2, 3, sd, null, params, "viewOriginalText",
                "viewExpandedText", "MANAGED_TABLE");
        groundStore.alterTable(DB1, TABLE1, newTbl1);
        tables = groundStore.getTables(DB1, "new*");
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals("new" + TABLE1, tables.get(0));

        groundStore.dropTable(DB1, "new" + TABLE1);
        tables = groundStore.getAllTables(DB1);
        Assert.assertEquals(0, tables.size());

        groundStore.dropDatabase(DB1);
    }

    /**
     * Tests partition operations
     */
    @Test
    public void testPartitionOps()
            throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
        Database db1 = new Database(DB1, "description", "locationurl", null);
        groundStore.createDatabase(db1);
        StorageDescriptor sd = new StorageDescriptor(null, "location", null, null, false, 0,
                new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
        HashMap<String, String> tableParams = new HashMap<String, String>();
        tableParams.put("EXTERNAL", "false");
        FieldSchema partitionKey1 = new FieldSchema("Country", serdeConstants.STRING_TYPE_NAME, "");
        FieldSchema partitionKey2 = new FieldSchema("State", serdeConstants.STRING_TYPE_NAME, "");
        Table tbl1 = new Table(TABLE1, DB1, "owner", 1, 2, 3, sd, Arrays.asList(partitionKey1, partitionKey2),
                tableParams, "viewOriginalText", "viewExpandedText", "MANAGED_TABLE");
        groundStore.createTable(tbl1);
        HashMap<String, String> partitionParams = new HashMap<String, String>();
        partitionParams.put("PARTITION_LEVEL_PRIVILEGE", "true");
        List<String> value1 = Arrays.asList("US", "CA");
        Partition part1 = new Partition(value1, DB1, TABLE1, 111, 111, sd, partitionParams);
        groundStore.addPartition(part1);
        List<String> value2 = Arrays.asList("US", "MA");
        Partition part2 = new Partition(value2, DB1, TABLE1, 222, 222, sd, partitionParams);
        groundStore.addPartition(part2);

        Deadline.startTimer("getPartition");
        List<Partition> partitions = groundStore.getPartitions(DB1, TABLE1, 10);
        Assert.assertEquals(2, partitions.size());
        Assert.assertEquals(111, partitions.get(0).getCreateTime());
        Assert.assertEquals(222, partitions.get(1).getCreateTime());

        int numPartitions = groundStore.getNumPartitionsByFilter(DB1, TABLE1, "");
        Assert.assertEquals(partitions.size(), numPartitions);

        numPartitions = groundStore.getNumPartitionsByFilter(DB1, TABLE1, "country = \"US\"");
        Assert.assertEquals(2, numPartitions);

        groundStore.dropPartition(DB1, TABLE1, value1);
        partitions = groundStore.getPartitions(DB1, TABLE1, 10);
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(222, partitions.get(0).getCreateTime());

        groundStore.dropPartition(DB1, TABLE1, value2);
        groundStore.dropTable(DB1, TABLE1);
        groundStore.dropDatabase(DB1);
    }

    /**
     * Test master keys operation
     */
    @Test
    public void testMasterKeyOps() throws MetaException, NoSuchObjectException {
        int id1 = groundStore.addMasterKey(KEY1);
        int id2 = groundStore.addMasterKey(KEY2);

        String[] keys = groundStore.getMasterKeys();
        Assert.assertEquals(2, keys.length);
        Assert.assertEquals(KEY1, keys[0]);
        Assert.assertEquals(KEY2, keys[1]);

        groundStore.updateMasterKey(id1, "new" + KEY1);
        groundStore.updateMasterKey(id2, "new" + KEY2);
        keys = groundStore.getMasterKeys();
        Assert.assertEquals(2, keys.length);
        Assert.assertEquals("new" + KEY1, keys[0]);
        Assert.assertEquals("new" + KEY2, keys[1]);

        groundStore.removeMasterKey(id1);
        keys = groundStore.getMasterKeys();
        Assert.assertEquals(1, keys.length);
        Assert.assertEquals("new" + KEY2, keys[0]);

        groundStore.removeMasterKey(id2);
    }

    /**
     * Test role operation
     */
    @Test
    public void testRoleOps() throws InvalidObjectException, MetaException, NoSuchObjectException {
        groundStore.addRole(ROLE1, OWNER);
        groundStore.addRole(ROLE2, OWNER);
        List<String> roles = groundStore.listRoleNames();
        Assert.assertEquals(2, roles.size());
        Assert.assertEquals(ROLE2, roles.get(1));
        Role role1 = groundStore.getRole(ROLE1);
        Assert.assertEquals(OWNER, role1.getOwnerName());
        groundStore.grantRole(role1, USER1, PrincipalType.USER, OWNER, PrincipalType.ROLE, true);
        groundStore.revokeRole(role1, USER1, PrincipalType.USER, false);
        groundStore.removeRole(ROLE1);
    }

    public static void dropAllStoreObjects(GroundStore store)
            throws MetaException, InvalidObjectException, InvalidInputException {
        try {
            Deadline.registerIfNot(100000);
            List<String> dbs = store.getAllDatabases();
            for (int i = 0; i < dbs.size(); i++) {
                String db = dbs.get(i);
                List<String> tbls = store.getAllTables(db);
                for (String tbl : tbls) {
                    Deadline.startTimer("getPartition");
                    List<Partition> parts = store.getPartitions(db, tbl, 100);
                    for (Partition part : parts) {
                        store.dropPartition(db, tbl, part.getValues());
                    }
                    store.dropTable(db, tbl);
                }
                store.dropDatabase(db);
            }
            List<String> roles = store.listRoleNames();
            for (String role : roles) {
                store.removeRole(role);
            }
        } catch (NoSuchObjectException e) {
        }
    }
}
