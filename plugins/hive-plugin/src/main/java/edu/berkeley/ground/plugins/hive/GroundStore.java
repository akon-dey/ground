package edu.berkeley.ground.plugins.hive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.*; //TODO(krishna) fix
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.exceptions.GroundException;

public class GroundStore implements RawStore, Configurable {

    private static final String DEFAULT_VERSION = "1.0.0";

    static final private Logger LOG = LoggerFactory.getLogger(GroundStore.class.getName());

    private static final String DB_STATE = "_DATABASE_STATE";

    // Do not access this directly, call getHBase to make sure it is
    // initialized.
    private GroundReadWrite ground = null;
    private Configuration conf;
    private int txnNestLevel;

    public static enum EntityState {
        ACTIVE, DELETED;
    }

    public GroundStore() {
        ground = getGround();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = configuration;
    }

    @Override
    public void shutdown() {
        try {
            if (txnNestLevel != 0)
                rollbackTransaction();
            getGround().close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean openTransaction() {
        if (txnNestLevel++ <= 0) {
            LOG.debug("Opening Ground transaction");
            getGround().begin();
            txnNestLevel = 1;
        }
        return true;
    }

    @Override
    public boolean commitTransaction() {
        return true;
        // throw new UnsupportedOperationException();
    }

    @Override
    public void rollbackTransaction() {
        // throw new UnsupportedOperationException();
    }

    /**
     * create a database using ground APIs. Uses node and node version.
     */
    @Override
    public void createDatabase(Database db) throws InvalidObjectException, MetaException {
        NodeFactory nf = getGround().getNodeFactory();
        NodeVersionFactory nvf = getGround().getNodeVersionFactory();
        //check if database node exists if yes return
        try {
            NodeVersion nodeVersion = getNodeVersion(db.getName());
            //TODO Retreive version from node, bump up version and create a new node
            return;
        } catch (NoSuchObjectException e1) {
            //do nothing proceed and create the new DB
        }
        Database dbCopy = db.deepCopy();
        try {
            edu.berkeley.ground.api.versions.Type dbType =
                    edu.berkeley.ground.api.versions.Type.fromString("string");
            String dbName = HiveStringUtils.normalizeIdentifier(dbCopy.getName());
            NodeVersion n = createDatabaseNodeVersion(nf, nvf, dbCopy, dbType, dbName);
        } catch (GroundException e) {
            LOG.error("error creating database " + e);
            throw new MetaException(e.getMessage());
        }
    }

    private NodeVersion createDatabaseNodeVersion(NodeFactory nf, NodeVersionFactory nvf, Database dbCopy,
            edu.berkeley.ground.api.versions.Type dbType, String dbName) throws GroundException {
        Gson gson = new Gson();
        Tag dbTag = createTag(dbName, gson.toJson(dbCopy));
        //remove Tag dbTag = new Tag(DEFAULT_VERSION, dbName, Optional.of(dbCopy), Optional.of(dbType)); // fix
        Optional<String> reference = Optional.of(dbCopy.getLocationUri());
        Optional<String> structureVersionId = Optional.empty();
        Optional<String> parentId = Optional.empty();
        HashMap<String, Tag> tags = new HashMap<>();
        tags.put(dbName, dbTag);
        Tag stateTag = createTag(dbName, EntityState.ACTIVE.name());
        tags.put(DB_STATE, stateTag);
        //create a new tag map and populate all DB related metadata
        Optional<Map<String, Tag>> tagsMap = Optional.of(tags);
        Map<String, String> dbParamMap = dbCopy.getParameters();
        if (dbParamMap == null) {
            dbParamMap = new HashMap<String, String>();
        }
        Optional<Map<String, String>> parameters = Optional.of(dbParamMap);
        String nodeId = nf.create(dbName).getId();
        return nvf.create(tagsMap, structureVersionId, reference, parameters, nodeId, parentId);
    }

    @Override
    public Database getDatabase(String dbName) throws NoSuchObjectException {
        NodeVersion databaseNodeVersion = getNodeVersion(dbName);
        if (databaseNodeVersion == null) {
            return null;
        }
        Map<String, Tag> dbTag = databaseNodeVersion.getTags().get();
        String dbJson = (String) dbTag.get(dbName).getValue().get();
        return (Database) createMetastoreObject(dbJson, Database.class);
    }

    @Override
    public boolean dropDatabase(String dbName) throws NoSuchObjectException, MetaException {
        NodeVersion databaseNodeVersion = getNodeVersion(dbName);
        Map<String, Tag> dbTag = databaseNodeVersion.getTags().get();
        String state = (String) dbTag.get(DB_STATE).getValue().get();
        if (state.equals(EntityState.ACTIVE.name())) {
            Tag stateTag = createTag(dbName, EntityState.DELETED.name());
            HashMap<String, Tag> tags = new HashMap<>();
            tags.put(DB_STATE, stateTag);
            //TODO create Tombstone node after bumping up version
        }
        LOG.info("database deleted: {}, {}", dbName,
                databaseNodeVersion.getNodeId());
        return true;
    }

    @Override
    public boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException, MetaException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<String> getDatabases(String pattern) throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> getAllDatabases() throws MetaException {
        List<String> list = new ArrayList<>();
        return list;
    }

    @Override
    public boolean createType(Type type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getType(String typeName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropType(String typeName) {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * There would be a "database contains" relationship between D and T, and
     * there would be a "table contains" relationship between T and each
     * attribute. The types of attributes of those nodes, and the fact that A2
     * and A4 are partition keys would be tags of those nodes. The fact that the
     * table T is in a particular file format (Parquet or Avro) would be a tag
     * on the table node.
     * 
     */
    public void createTable(Table tbl) throws InvalidObjectException, MetaException {
        openTransaction();
        // HiveMetaStore above us checks if the table already exists, so we can
        // blindly store it here
        String dbName = tbl.getDbName();
        String tableName = tbl.getTableName();
        try {
            try {
                getNodeVersion(tbl.getTableName());
                return;
            } catch (NoSuchObjectException e) {
                //do nothing try creating a new node version
            }
            Table tblCopy = tbl.deepCopy();
            Map<String, Tag> tagsMap = new HashMap<>();
            tblCopy.setDbName(HiveStringUtils.normalizeIdentifier(dbName));
            tblCopy.setTableName(HiveStringUtils.normalizeIdentifier(tblCopy.getTableName()));
            normalizeColumnNames(tblCopy);
            NodeVersion tableNodeVersion = createTableNodeVersion(tblCopy, dbName, tableName, tagsMap);
            ObjectPair<String, Object> tableState =
                    new ObjectPair<>(tableNodeVersion.getId(), EntityState.ACTIVE);
            updateTableMetadata(tblCopy, dbName, tableName, tableState);
        } catch (GroundException e) {
            LOG.error("Unable to create table {}  {}", dbName, tableName);
            throw new MetaException("Unable to read from or write ground database " + e.getMessage());
        }
    }

    private void updateTableMetadata(Table tblCopy, String dbName, String tableName,
            ObjectPair<String, Object> tableState) {
        synchronized (ground.getDbTableMap()) {
            Map<String, Map<String, ObjectPair<String, Object>>> dbTable = ground.getDbTableMap();
            if (dbTable.containsKey(dbName)) {
                dbTable.get(dbName).put(tblCopy.getTableName(),
                        tableState);
            } else {
                Map<String, ObjectPair<String, Object>> tableMap =
                        new HashMap<String, ObjectPair<String, Object>>();
                tableMap.put(tableName, tableState);
                dbTable.put(dbName, tableMap);
            }
        }
    }


    public boolean dropTable(String dbName, String tableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        EntityState state = (EntityState) getGround().getDbTableMap().get(dbName).get(tableName).getSecond();
        if (state != null && state.equals(EntityState.ACTIVE)) {
            //TODO create TOMBSTONE
            ObjectPair<String, Object> updatedState =
                    new ObjectPair<String, Object>(tableName, EntityState.DELETED);
            getGround().getDbTableMap().get(dbName).put(tableName, updatedState);
            return true;
        }
        return false;
    }

    @Override
    public Table getTable(String dbName, String tableName) throws MetaException {
        NodeVersion tableNodeVersion;
        try {
            Node node = getGround().getNodeFactory().retrieveFromDatabase(tableName);
            tableNodeVersion = getGround().getNodeVersionFactory().retrieveFromDatabase(node.getId());
        } catch (GroundException e) {
            LOG.error("get failed for database: {}, {} ", dbName, tableName);
            throw new MetaException(e.getMessage());
        }
        Map<String, Tag> tblTag = tableNodeVersion.getTags().get();
        String tblJson = (String) tblTag.get(dbName).getValue().get();
        return (Table) createMetastoreObject(tblJson, Table.class);
        // return (Table) tblTag.get(tableName).getValue().get();
    }

    @Override
    public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
        NodeVersionFactory nvf = getGround().getNodeVersionFactory();
        NodeFactory nf = getGround().getNodeFactory();
        try {
            Partition partCopy = part.deepCopy();
            String dbName = part.getDbName();
            String tableName = part.getTableName();
            partCopy.setDbName(HiveStringUtils.normalizeIdentifier(dbName));
            ObjectPair<String, String> objectPair = new ObjectPair<>(dbName, tableName);
            String partId = objectPair.toString();
            partCopy.setTableName(HiveStringUtils.normalizeIdentifier(tableName));
            // edu.berkeley.ground.api.versions.Type partType = edu.berkeley.ground.api.versions.Type.fromString("string");
            Gson gson = new Gson();
            Tag partTag = createTag(partId, gson.toJson(partCopy));
            Optional<String> reference = Optional.of(partCopy.getSd().getLocation());
            Optional<String> versionId = Optional.empty();
            Optional<String> parentId = Optional.empty(); // fix
            Map<String, Tag> tags = new HashMap<>();
            tags.put(partId, partTag);

            Optional<Map<String, String>> parameters = Optional.of(partCopy.getParameters());
            String nodeName = HiveStringUtils.normalizeIdentifier(partId + partCopy.getCreateTime());
            NodeVersion nodeVersion;
            try {
                nodeVersion = getNodeVersion(nodeName);
                if (nodeVersion != null) {
                    //part exists return
                    return false;
                }
            } catch (NoSuchObjectException e) {
                // do nothing here - continue to create a new partition
            }

            String nodeId = nf.create(nodeName).getId();
            Optional<Map<String, Tag>> tagsMap = Optional.of(tags);
            LOG.info("input partition from tag map: {} {}", tagsMap.get().get(partId).getKey(),
                    tagsMap.get().get(partId).getValue());
            NodeVersion n = nvf.create(tagsMap, versionId, reference, parameters, nodeId, parentId);
            List<String> partList = ground.getPartCache().get(objectPair);
            if (partList == null) {
                partList = new ArrayList<>();
            }
            String partitionNodeId = n.getId();
            partList.add(partitionNodeId);
            LOG.info("adding partition: {} {}", objectPair, partitionNodeId);
            ground.getPartCache().put(objectPair, partList);// TODO use hive PartitionCache
            LOG.info("partition list size {}", partList.size());
            return true;
        } catch (GroundException e) {
            throw new MetaException("Unable to add partition " + e.getMessage());
        }
    }

    public boolean addPartitions(String dbName, String tblName, List<Partition> parts)
            throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean addPartitions(String dbName, String tblName, PartitionSpecProxy partitionSpec, boolean ifNotExists)
            throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> part_vals)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean doesPartitionExist(String dbName, String tableName, List<String> part_vals)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean dropPartition(String dbName, String tableName, List<String> part_vals)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tableName, int max)
            throws MetaException, NoSuchObjectException {
        ObjectPair<String, String> pair = new ObjectPair<>(dbName, tableName);
        List<String> idList = ground.getPartCache().get(pair);
        int size = max <= idList.size() ? max : idList.size();
        LOG.debug("size of partition array: {} {}", size, idList.size());
        List<String> subPartlist = idList.subList(0, size);
        List<Partition> partList = new ArrayList<Partition>();
        for (String id : subPartlist) {
            partList.add(getPartition(id));
        }
        return partList;
    }

    public void alterTable(String dbname, String name, Table newTable) throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub

    }

    public List<String> getTables(String dbName, String pattern) throws MetaException {
        return getAllTables(dbName); //fix regex
    }

    public List<TableMeta> getTableMeta(String dbNames, String tableNames, List<String> tableTypes)
            throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<Table> getTableObjectsByName(String dbname, List<String> tableNames)
            throws MetaException, UnknownDBException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> getAllTables(String dbName) throws MetaException {
        ArrayList<String> list = new ArrayList<String>();
        EdgeVersionFactory evf = getGround().getEdgeVersionFactory();
        // list.addAll(ground.getDbTableMap().get(dbName).keySet());
        return list;
    }

    public List<String> listTableNamesByFilter(String dbName, String filter, short max_tables)
            throws MetaException, UnknownDBException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts) throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> listPartitionNamesByFilter(String db_name, String tbl_name, String filter, short max_parts)
            throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public void alterPartition(String db_name, String tbl_name, List<String> part_vals, Partition new_part)
            throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub
    }

    public void alterPartitions(String db_name, String tbl_name, List<List<String>> part_vals_list,
            List<Partition> new_parts) throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub

    }

    public boolean addIndex(Index index) throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub
        return false;
    }

    public Index getIndex(String dbName, String origTableName, String indexName) throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean dropIndex(String dbName, String origTableName, String indexName) throws MetaException {
        // TODO Auto-generated method stub
        return false;
    }

    public List<Index> getIndexes(String dbName, String origTableName, int max) throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> listIndexNames(String dbName, String origTableName, short max) throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public void alterIndex(String dbname, String baseTblName, String name, Index newIndex)
            throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub

    }

    public List<Partition> getPartitionsByFilter(String dbName, String tblName, String filter, short maxParts)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr, String defaultPartitionName,
            short maxParts, List<Partition> result) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    public int getNumPartitionsByFilter(String dbName, String tblName, String filter)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return 0;
    }

    public List<Partition> getPartitionsByNames(String dbName, String tblName, List<String> partNames)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    public Table markPartitionForEvent(String dbName, String tblName, Map<String, String> partVals,
            PartitionEventType evtType)
            throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isPartitionMarkedForEvent(String dbName, String tblName, Map<String, String> partName,
            PartitionEventType evtType)
            throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean addRole(String rowName, String ownerName)
            throws InvalidObjectException, MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean grantRole(Role role, String userName, PrincipalType principalType, String grantor,
            PrincipalType grantorType, boolean grantOption)
            throws MetaException, NoSuchObjectException, InvalidObjectException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean revokeRole(Role role, String userName, PrincipalType principalType, boolean grantOption)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return false;
    }

    public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
            throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName, List<String> groupNames)
            throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName, String tableName, String userName,
            List<String> groupNames) throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName, String tableName, String partition,
            String userName, List<String> groupNames) throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName, String tableName, String partitionName,
            String columnName, String userName, List<String> groupNames) throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName, PrincipalType principalType) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName, PrincipalType principalType,
            String dbName) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listAllTableGrants(String principalName, PrincipalType principalType,
            String dbName, String tableName) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName, PrincipalType principalType,
            String dbName, String tableName, List<String> partValues, String partName) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName, PrincipalType principalType,
            String dbName, String tableName, String columnName) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName,
            PrincipalType principalType, String dbName, String tableName, List<String> partValues, String partName,
            String columnName) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean grantPrivileges(PrivilegeBag privileges)
            throws InvalidObjectException, MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
            throws InvalidObjectException, MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return false;
    }

    public Role getRole(String roleName) throws NoSuchObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> listRoleNames() {
        return new ArrayList<>();
    }

    public List<Role> listRoles(String principalName, PrincipalType principalType) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<RolePrincipalGrant> listRolesWithGrants(String principalName, PrincipalType principalType) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<RolePrincipalGrant> listRoleMembers(String roleName) {
        // TODO Auto-generated method stub
        return null;
    }

    public Partition getPartitionWithAuth(String dbName, String tblName, List<String> partVals, String user_name,
            List<String> group_names) throws MetaException, NoSuchObjectException, InvalidObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<Partition> getPartitionsWithAuth(String dbName, String tblName, short maxParts, String userName,
            List<String> groupNames) throws MetaException, NoSuchObjectException, InvalidObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> listPartitionNamesPs(String db_name, String tbl_name, List<String> part_vals, short max_parts)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name, List<String> part_vals,
            short max_parts, String userName, List<String> groupNames)
            throws MetaException, InvalidObjectException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean updateTableColumnStatistics(ColumnStatistics colStats)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj, List<String> partVals)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        // TODO Auto-generated method stub
        return false;
    }

    public ColumnStatistics getTableColumnStatistics(String dbName, String tableName, List<String> colName)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<ColumnStatistics> getPartitionColumnStatistics(String dbName, String tblName, List<String> partNames,
            List<String> colNames) throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
            List<String> partVals, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
        // TODO Auto-generated method stub
        return false;
    }

    public long cleanupEvents() {
        // TODO Auto-generated method stub
        return 0;
    }

    public boolean addToken(String tokenIdentifier, String delegationToken) {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean removeToken(String tokenIdentifier) {
        // TODO Auto-generated method stub
        return false;
    }

    public String getToken(String tokenIdentifier) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> getAllTokenIdentifiers() {
        // TODO Auto-generated method stub
        return null;
    }

    public int addMasterKey(String key) throws MetaException {
        // TODO Auto-generated method stub
        return 0;
    }

    public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException {
        // TODO Auto-generated method stub
    }

    public boolean removeMasterKey(Integer keySeq) {
        // TODO Auto-generated method stub
        return false;
    }

    public String[] getMasterKeys() {
        // TODO Auto-generated method stub
        return null;
    }

    public void verifySchema() throws MetaException {
        // TODO Auto-generated method stub

    }

    public String getMetaStoreSchemaVersion() throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public void setMetaStoreSchemaVersion(String version, String comment) throws MetaException {
        // TODO Auto-generated method stub
    }

    public void dropPartitions(String dbName, String tblName, List<String> partNames)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub

    }

    public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String principalName, PrincipalType principalType) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName, PrincipalType principalType) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String principalName,
            PrincipalType principalType) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String principalName,
            PrincipalType principalType) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(String principalName,
            PrincipalType principalType) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listGlobalGrantsAll() {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listDBGrantsAll(String dbName) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String dbName, String tableName, String partitionName,
            String columnName) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listTableGrantsAll(String dbName, String tableName) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listPartitionGrantsAll(String dbName, String tableName, String partitionName) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<HiveObjectPrivilege> listTableColumnGrantsAll(String dbName, String tableName, String columnName) {
        // TODO Auto-generated method stub
        return null;
    }

    public void createFunction(Function func) throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub
    }

    public void alterFunction(String dbName, String funcName, Function newFunction)
            throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub
    }

    public void dropFunction(String dbName, String funcName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        // TODO Auto-generated method stub
    }

    public Function getFunction(String dbName, String funcName) throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<Function> getAllFunctions() throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<String> getFunctions(String dbName, String pattern) throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public AggrStats get_aggr_stats_for(String dbName, String tblName, List<String> partNames, List<String> colNames)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
        // TODO Auto-generated method stub
        return null;
    }

    public void addNotificationEvent(NotificationEvent event) {
        // TODO Auto-generated method stub
    }

    public void cleanNotificationEvents(int olderThan) {
        // TODO Auto-generated method stub
    }

    public CurrentNotificationEventId getCurrentNotificationEventId() {
        // TODO Auto-generated method stub
        return null;
    }

    public void flushCache() {
        // TODO Auto-generated method stub
    }

    public ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata, FileMetadataExprType type)
            throws MetaException {
        // TODO Auto-generated method stub
    }

    public boolean isFileMetadataSupported() {
        // TODO Auto-generated method stub
        return false;
    }

    public void getFileMetadataByExpr(List<Long> fileIds, FileMetadataExprType type, byte[] expr,
            ByteBuffer[] metadatas, ByteBuffer[] exprResults, boolean[] eliminated) throws MetaException {
        // TODO Auto-generated method stub
    }

    public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
        // TODO Auto-generated method stub
        return null;
    }

    public int getTableCount() throws MetaException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getPartitionCount() throws MetaException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getDatabaseCount() throws MetaException {
        // TODO Auto-generated method stub
        return 0;
    }

    public List<SQLPrimaryKey> getPrimaryKeys(String db_name, String tbl_name) throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public List<SQLForeignKey> getForeignKeys(String parent_db_name, String parent_tbl_name, String foreign_db_name,
            String foreign_tbl_name) throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    public void createTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys)
            throws InvalidObjectException, MetaException {
        // TODO (FIX)
        createTable(tbl);
    }

    public void dropConstraint(String dbName, String tableName, String constraintName) throws NoSuchObjectException {
        // TODO Auto-generated method stub

    }

    public void addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub

    }

    @Override
    public void addForeignKeys(List<SQLForeignKey> fks) throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub

    }

    /** Given an entity name retrieve its node version from database. */
    private NodeVersion getNodeVersion(String name) throws NoSuchObjectException {
        try {
            List<String> versions = getGround().getNodeFactory().getLeaves(name);
            if (versions == null || versions.isEmpty())
                return null;
            return getGround().getNodeVersionFactory().retrieveFromDatabase(versions.get(0));
        } catch (GroundException e) {
            LOG.error("get failed for database {}", name);
            throw new NoSuchObjectException(e.getMessage());
        }
    }

    private <T> Object createMetastoreObject(String dbJson, Class<T> klass) {
        Gson gson = new Gson();
        return gson.fromJson(dbJson, klass);
    }

    private GroundReadWrite getGround() {
        if (ground == null) {
            if (conf == null) {
                conf = new HiveConf();
            }
            GroundReadWrite.setConf(conf);
            this.ground = GroundReadWrite.getInstance();
        }
        return ground;
    }


    /** Create node version for the given table. */
    private NodeVersion createTableNodeVersion(Table tblCopy, String dbName, String tableName,
            Map<String, Tag> tagsMap) throws GroundException {
        Gson gson = new Gson();
        Tag tblTag = createTag(tableName, gson.toJson(tblCopy));
        tagsMap.put(tableName, tblTag);
        // create an edge to db which contains this table
        EdgeVersionFactory evf = getGround().getEdgeVersionFactory();
        EdgeFactory ef = getGround().getEdgeFactory();

        Optional<Map<String, Tag>> tags = Optional.of(tagsMap);
        Optional<Map<String, String>> parameters = Optional.of(tblCopy.getParameters());
        // new node for this table
        String nodeId = getGround().getNodeFactory().create(tableName).getId();
        NodeVersion tableNodeVersion = getGround().getNodeVersionFactory().create(tags, Optional.empty(), Optional.empty(),
                parameters, nodeId, Optional.empty());
        Edge edge = ef.create(dbName); //use data base (parent) name as edge identifier
        //get Database NodeVersion - this will define from Node for the edge
        String dbNodeId;
        try {
            dbNodeId = getNodeVersion(dbName).getId();
        } catch (NoSuchObjectException e) {
            LOG.error("error retrieving database node from ground store {}", dbName);
            throw new GroundException(e);
        }
        EdgeVersion ev = evf.create(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
                edge.getId(), dbNodeId, tableNodeVersion.getId(), Optional.empty());
        return tableNodeVersion;
    }

    private Tag createTag(String id, Object value) {
        return createTag(DEFAULT_VERSION, id, value, Optional.of(edu.berkeley.ground.api.versions.Type.STRING));
    }

    private Tag createTag(String version, String id, Object value,
            Optional<edu.berkeley.ground.api.versions.Type> type) {
        return new Tag(version, id, Optional.of(value), type);
    }

    /**
     * Using utilities from Hive HBaseStore
     * 
     * @param tbl
     */
    private void normalizeColumnNames(Table tbl) {
        if (tbl.getSd().getCols() != null) {
            tbl.getSd().setCols(normalizeFieldSchemaList(tbl.getSd().getCols()));
        }
        if (tbl.getPartitionKeys() != null) {
            tbl.setPartitionKeys(normalizeFieldSchemaList(tbl.getPartitionKeys()));
        }
    }

    private List<FieldSchema> normalizeFieldSchemaList(List<FieldSchema> fieldschemas) {
        List<FieldSchema> ret = new ArrayList<>();
        for (FieldSchema fieldSchema : fieldschemas) {
            ret.add(new FieldSchema(fieldSchema.getName().toLowerCase(), fieldSchema.getType(),
                    fieldSchema.getComment()));
        }
        return ret;
    }

    // use NodeVersion ID to retrieve serialized partition string from Ground backend
    private Partition getPartition(String id) throws MetaException, NoSuchObjectException {
        NodeVersion partitonNodeVersion;
        try {
            partitonNodeVersion = getGround().getNodeVersionFactory().retrieveFromDatabase(id);
            LOG.debug("node id {}", partitonNodeVersion.getId());
        } catch (GroundException e) {
            LOG.error("get failed for id:{}", id);
            throw new MetaException(e.getMessage());
        }

        Collection<Tag> partTags = partitonNodeVersion.getTags().get().values();
        List<Partition> partList = new ArrayList<Partition>();
        for (Tag t : partTags) {
            String partitionString =  (String) t.getValue().get();
            Partition partition = (Partition) createMetastoreObject(partitionString,
                    Partition.class);
            partList.add(partition);
        }
        return partList.get(0);
    }
}
