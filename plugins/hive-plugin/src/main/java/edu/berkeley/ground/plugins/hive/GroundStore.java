package edu.berkeley.ground.plugins.hive;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroundStore extends GroundMetaStore {

    private static final String DEFAULT_VERSION = "1.0";

    static final private Logger LOG = LoggerFactory.getLogger(GroundMetaStore.class.getName());

    private static final String DB_STATE = "_DATABASE_STATE";
    private static final String DB_VERSION = "_DB_VERSION";

    private static final String TABLE_STATE = "_TABLE_STATE";
    private static final String TABLE_VERSION = "_TABLE_VERSION";

    private static final String METASTORE_NODE = "_METASTORE";

    // Do not access this directly, call getGround to make sure it is
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

    @Override
    public boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException, MetaException {
        if (dbname == null || dbname.isEmpty() || db == null)
            throw new NoSuchObjectException("Unable to locate database " + dbname + " with " + db);
        try {
            dropDatabase(dbname);
            createDatabase(db);
        } catch (NoSuchObjectException | MetaException ex) {
            LOG.debug("Alter database failed with: " + ex.getMessage());
            throw ex;
        } catch (InvalidObjectException ex) {
            LOG.debug("Alter database failed with: " + ex.getMessage());
            throw new MetaException("Alter database failed: " + ex.getMessage());
        }
        return false;
    }

    @Override
    public List<String> getDatabases(String dbPattern) throws MetaException {
        List<String> databases = new ArrayList<String>();
        try {
            List<String> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

            if (!versions.isEmpty()) {
                String metaVersionId = versions.get(0);
                List<String> dbNodeIds = ground.getNodeVersionFactory().getAdjacentNodes(metaVersionId, dbPattern);
                for (String dbNodeId : dbNodeIds) {
                    NodeVersion dbNodeVersion = ground.getNodeVersionFactory().retrieveFromDatabase(dbNodeId);
                    // fetch the edge for the dbname
                    Edge edge = ground.getEdgeFactory().retrieveFromDatabase(dbNodeVersion.getNodeId());
                    databases.add(edge.getName().split("Nodes.")[1]);
                }
            }
        } catch (GroundException ex) {
            LOG.error("Get databases failed for pattern {}", dbPattern);
            throw new MetaException(ex.getMessage());
        }
        return databases;
    }

    @Override
    public List<String> getAllDatabases() throws MetaException {
        try {
            return this.getDatabases("");
        } catch (MetaException ex) {
            LOG.error("Failed to get all databases");
            throw new MetaException(ex);
        }
    }

    /**
     * create a database using ground APIs. Uses node and node version.
     */
    @Override
    public void createDatabase(Database db) throws InvalidObjectException, MetaException {
        NodeFactory nf = getGround().getNodeFactory();
        NodeVersionFactory nvf = getGround().getNodeVersionFactory();
        // TODO create metanode first if it does not exist
        // then check if database node exists if yes return
        try {
            String dbName = db.getName();
            NodeVersion nodeVersion = getNodeVersion(dbName);
            boolean checkStatus = checkNodeStatus(nodeVersion, DB_STATE);
            if (checkStatus) { //if state is "DELETED" create a new version
                Database dbCopy = db.deepCopy();
                String version = nodeVersion.getTags().get(dbName).getVersionId();
                int current = new Integer(version);
                String updatedVersion = new Integer(current + 1).toString();
                GroundType dbType = GroundType.fromString("string");
                createDatabaseNodeVersion(updatedVersion, nf, nvf, dbCopy, dbType, db.getName());
                return;
            }
        } catch (NoSuchObjectException | GroundException e1) {
            // do nothing proceed and create the new DB
        }
        Database dbCopy = db.deepCopy();
        try {
            GroundType dbType = GroundType.fromString("string");
            String dbName = HiveStringUtils.normalizeIdentifier(dbCopy.getName());
            String version = getCurrentVersion(dbName);
            NodeVersion n = createDatabaseNodeVersion(version, nf, nvf, dbCopy, dbType, dbName);
        } catch (GroundException e) {
            LOG.error("error creating database " + e);
            throw new MetaException(e.getMessage());
        }
    }

    @Override
    public Database getDatabase(String dbName) throws NoSuchObjectException {
        NodeVersion databaseNodeVersion = getNodeVersion(dbName);
        if (databaseNodeVersion == null) {
            LOG.info("database node version is not present");
            return null;
        }
        Map<String, Tag> dbTag = databaseNodeVersion.getTags();
        String dbJson = (String) dbTag.get(dbName).getValue();
        return (Database) createMetastoreObject(dbJson, Database.class);
    }

    @Override
    public boolean dropDatabase(String dbName) throws NoSuchObjectException, MetaException {
        NodeVersion databaseNodeVersion = getNodeVersion(dbName);
        Map<String, Tag> dbTag = databaseNodeVersion.getTags();
        if (dbTag == null) {
            LOG.info("node version getTags failed");
            return false;
        }
        String state = (String) dbTag.get(dbName + DB_STATE).getValue();
        if (state.equals(EntityState.ACTIVE.name())) {
            try {
                createTombstone(dbName, DB_STATE, DB_VERSION, dbTag);
            } catch (GroundException e) {
                throw new MetaException(e.getMessage());
            }
        }
        LOG.info("database deleted: {}, {}", dbName, databaseNodeVersion.getNodeId());
        return true;
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
               if (getNodeVersion(tbl.getTableName()) != null) {
                   //table exists - check its state and create as needed
                   //TODO(krishna) state check
                   return;
               }
           } catch (NoSuchObjectException e) {
               // do nothing try creating a new node version
           }
           Table tblCopy = tbl.deepCopy();
           Map<String, Tag> tagsMap = new HashMap<>();
           tblCopy.setDbName(HiveStringUtils.normalizeIdentifier(dbName));
           tblCopy.setTableName(HiveStringUtils.normalizeIdentifier(tblCopy.getTableName()));
           normalizeColumnNames(tblCopy);
           NodeVersion tableNodeVersion = createTableNodeVersion(tblCopy, dbName, tableName, tagsMap);
       } catch (GroundException e) {
           LOG.error("Unable to create table {}  {}", dbName, tableName);
           throw new MetaException("Unable to read from or write ground database " + e.getMessage());
       }
   }

   @Override
   public boolean dropTable(String dbName, String tableName)
           throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
       NodeVersion tableNodeVersion = getNodeVersion(tableName); //TODO use database as well use edge/construct from ground
       Map<String, Tag> tblTag = tableNodeVersion.getTags();
       if (tblTag == null) {
           LOG.info("node version getTags failed");
           return false;
       }
       
       String state = (String) tblTag.get(tableName + TABLE_STATE).getValue();
       if (state.equals(EntityState.ACTIVE.name())) {
           Tag stateTag = createTag(tableName + TABLE_STATE + "deleted", EntityState.DELETED.name());
           tblTag.put(DB_STATE, stateTag);
           try {
               createTableNodeVersion(DEFAULT_VERSION, null, dbName, tableName, tblTag);
           } catch (GroundException e) {
               throw new MetaException (e.getMessage());
           }
           return true;
       }
       return false;
   }

   @Override
   public Table getTable(String dbName, String tableName) throws MetaException {
       NodeVersion tableNodeVersion;
       try {
           LOG.info("getting versions for table {}", tableName);
           List<String> versions = getGround().getNodeFactory().getLeaves(tableName);
           tableNodeVersion = getGround().getNodeVersionFactory().retrieveFromDatabase(versions.get(0));
       } catch (GroundException e) {
           LOG.error("get failed for database: {}, {} ", dbName, tableName);
           throw new MetaException(e.getMessage());
       }
       Map<String, Tag> tblTag = tableNodeVersion.getTags();
       String tblJson = (String) tblTag.get(tableName).getValue();
       LOG.info("table serialized data is " + tblJson);
       return (Table) createMetastoreObject(tblJson, Table.class);
       // return (Table) tblTag.get(tableName).getValue().get();
   }

   @Override
   public List<String> getAllTables(String dbName) throws MetaException {
       ArrayList<String> list = new ArrayList<String>();
       EdgeVersionFactory evf = getGround().getEdgeVersionFactory();
       return list;
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
           Gson gson = new Gson();
           Tag partTag;
           String nodeName = HiveStringUtils.normalizeIdentifier(partId +
                   partCopy.getCreateTime());
           String nodeId = null;
           try {
               nodeId = nf.retrieveFromDatabase(nodeName).getId();
           } catch (GroundException e) {
               LOG.info("node not there in database create new");
           }
           if (nodeId == null) {
               nodeId = nf.create(nodeName).getId();
           }

           String reference = partCopy.getSd().getLocation();
           List<String> parents = new ArrayList<>();
           Map<String, Tag> tags = new HashMap<>();
           partTag = createTag(nodeName, gson.toJson(partCopy));
           tags.put(partId, partTag);
           Map<String, String> parameters = partCopy.getParameters();
           StructureVersion sv = createStructureVersion(nodeName, parents);

           LOG.info("input partition from tag map: {} {}", tags.get(partId).getKey(),
                   tags.get(partId).getValue());
           NodeVersion n = nvf.create(tags, sv.getId(), reference, parameters, nodeId, parents);
           List<String> partList = ground.getPartCache().get(objectPair);
           if (partList == null) {
               partList = new ArrayList<>();
           }
           String partitionNodeId = n.getId();
           partList.add(partitionNodeId);
           LOG.info("adding partition: {} {}", objectPair, partitionNodeId);
           ground.getPartCache().put(objectPair, partList);// TODO use hive
                                                           // PartitionCache
           LOG.info("partition list size {}", partList.size());
           return true;
       } catch (GroundException e) {
           throw new MetaException("Unable to add partition " + e.getMessage());
       }
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

   private String getCurrentVersion(String dbName) {
       //place holder for now. return default version
       return DEFAULT_VERSION;
   }

   /** Given an entity name retrieve its node version from database. */
   private NodeVersion getNodeVersion(String name) throws NoSuchObjectException {
       try {
           List<String> versions = getVersions(name);
           if (versions == null) {
               return null;
           }
           return getGround().getNodeVersionFactory().retrieveFromDatabase(versions.get(0));
       } catch (GroundException e) {
           LOG.error("get failed for database {}", name);
           throw new NoSuchObjectException(e.getMessage());
       }
   }

   private List<String> getVersions(String name) throws GroundException {
       LOG.info("getting node versions for {}", name);
       List<String> versions = getGround().getNodeFactory().getLeaves(name);
       if (versions == null || versions.isEmpty())
           return null;
       return versions;
   }

   private boolean checkNodeStatus(NodeVersion nodeVersion, String state) {
       if (nodeVersion == null) {
           LOG.debug("node version is not available");
           return false;
       }
       Map<String, Tag> tag = nodeVersion.getTags();
       if (!tag.isEmpty()) {
           Tag statusTag = tag.get(state);
           if (statusTag != null && statusTag.getValue().equals(EntityState.DELETED.name()))
               return true;
       }
       return false;
   }

   private NodeVersion createDatabaseNodeVersion(String version, NodeFactory nf, NodeVersionFactory nvf,
           Database dbCopy, GroundType dbType, String dbName) throws GroundException {
       Gson gson = new Gson();
       Tag dbTag = null;
       if (dbCopy == null) {// create a tombstone node
           dbTag = createTag(version, dbName, gson.toJson(""), GroundType.STRING);
       } else {
           dbTag = createTag(version, dbName, gson.toJson(dbCopy), GroundType.STRING);
       }

       List<String> parentId = new ArrayList<>();
       StructureVersion sv = createStructureVersion(dbName, parentId);
       String nodeId = nf.create(dbName).getId();
       String reference = dbCopy.getLocationUri();
       HashMap<String, Tag> tags = new HashMap<>();
       tags.put(dbName, dbTag);
       Tag stateTag = createTag(dbName + DB_STATE, EntityState.ACTIVE.name());
       tags.put(DB_STATE, stateTag);
       Tag versionTag = createTag(dbName + DB_VERSION, version);
       tags.put(DB_VERSION, versionTag);
       // create a new tag map and populate all DB related metadata
       Map<String, String> dbParamMap = dbCopy.getParameters();
       if (dbParamMap == null) {
           dbParamMap = new HashMap<String, String>();
       }
       Map<String, String> parameters = dbParamMap;
       return nvf.create(tags, sv.getId(), reference, parameters, nodeId, parentId);
   }

   private StructureVersion createStructureVersion(String name, List<String> parentId) throws GroundException {
       Map<String,GroundType> structureVersionAttributes = new HashMap<>();
       structureVersionAttributes.put(name, GroundType.STRING);
       Structure structure = getGround().getStructureFactory().create(name);
       StructureVersion sv = getGround().getStructureVersionFactory().create(structure.getId(),
               structureVersionAttributes, parentId);
       return sv;
   }

   /** Create node version for the given table. */
   @VisibleForTesting
   protected NodeVersion createTableNodeVersion(Table tblCopy, String dbName, String tableName,
           Map<String, Tag> tagsMap) throws GroundException {
       return createTableNodeVersion(DEFAULT_VERSION, tblCopy, dbName, tableName, tagsMap);
   }

   /** Create node version for the given table. */
   private NodeVersion createTableNodeVersion(String version, Table tblCopy, String dbName, String tableName,
           Map<String, Tag> tagsMap) throws GroundException {
       Gson gson = new Gson();
       Tag tblTag = null;
       if (tblCopy == null) {// create a tombstone node
           return createTombstone(tableName, TABLE_STATE, TABLE_VERSION, tagsMap);
       }
       tblTag = createTag(version, tableName, gson.toJson(tblCopy), GroundType.STRING);
       tagsMap.put(tableName, tblTag);
       String id = tableName + TABLE_STATE;
       Tag stateTag = createTag(id, EntityState.ACTIVE.name());
       tagsMap.put(id, stateTag);
       Tag versionTag = createTag(tableName + TABLE_VERSION, version);
       tagsMap.put(tableName + TABLE_VERSION, versionTag);

       // create an edge to db which contains this table
       EdgeVersionFactory evf = getGround().getEdgeVersionFactory();
       EdgeFactory ef = getGround().getEdgeFactory();

       Map<String, Tag> tags = tagsMap;
       Map<String, String> parameters = tblCopy.getParameters();
       // new node for this table
       String nodeId = getGround().getNodeFactory().create(tableName).getId();
       List<String> parents = new ArrayList<>();
       // parents.add(dbName);
       String reference = tblCopy.getOwner();
       StructureVersion sv = createStructureVersion(tableName, parents);
       NodeVersion tableNodeVersion = getGround().getNodeVersionFactory().create(tags, sv.getId(),
               reference, parameters, nodeId, parents);
       Edge edge = ef.create(dbName); // use data base (parent) name as edge
                                      // identifier
       // get Database NodeVersion - this will define from Node for the edge
       String dbNodeId;
       try {
           dbNodeId = getNodeVersion(dbName).getId();
       } catch (NoSuchObjectException e) {
           LOG.error("error retrieving database node from ground store {}", dbName);
           throw new GroundException(e);
       }
       EdgeVersion ev = evf.create(tags, sv.getId(), reference, parameters,
               edge.getId(), dbNodeId, tableNodeVersion.getId(), parents);
       return tableNodeVersion;
   }

   private NodeVersion createTombstone(String name, String state, String version, Map<String, Tag> tagsMap) throws GroundException {
       Tag tblTag;
       Tag versionTag = tagsMap.get(name + version);
       Double versionId = Float.parseFloat((String) versionTag.getValue()) + 1.0;
       Gson gson = new Gson();
       tblTag = createTag(versionId.toString(), name + EntityState.DELETED.name(), gson.toJson(""), GroundType.STRING);
       tagsMap.put(name, tblTag);
       String id = name + state;
       Tag stateTag = createTag(id, EntityState.DELETED.name());
       tagsMap.put(id, stateTag);
       String nodeId = getGround().getNodeFactory().retrieveFromDatabase(name).getId();
       Map<String, String> parameters = new HashMap<>();
       String reference = "deleted-table";
       List<String> parents = new ArrayList<>();
       // Structure structure = getGround().getStructureFactory().retrieveFromDatabase(tableName);
       // PostgresStructureVersionFactory psvf = (PostgresStructureVersionFactory) getGround().getStructureVersionFactory();
       // StructureVersion sv = psvf.retrieveFromDatabase(structure.getId());
       StructureVersion sv = createStructureVersion(name + EntityState.DELETED.name(), parents);
       return getGround().getNodeVersionFactory().create(tagsMap, sv.getId(),
               reference, parameters, nodeId, parents);
   }

   private Tag createTag(String id, Object value) {
       return createTag(DEFAULT_VERSION, id, value, GroundType.STRING);
   }

   private Tag createTag(String version, String id, Object value, GroundType type) {
       return new Tag(version, id, value, type);
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

   /**
    * Internal method to get a partition from database
    * @param id
    * @return
    * @throws MetaException
    * @throws NoSuchObjectException
    */
   private Partition getPartition(String id) throws MetaException, NoSuchObjectException {
       NodeVersion partitonNodeVersion;
       try {
           partitonNodeVersion = getGround().getNodeVersionFactory().retrieveFromDatabase(id);
           LOG.debug("node id {}", partitonNodeVersion.getId());
       } catch (GroundException e) {
           LOG.error("get failed for id:{}", id);
           throw new MetaException(e.getMessage());
       }

       Collection<Tag> partTags = partitonNodeVersion.getTags().values();
       List<Partition> partList = new ArrayList<Partition>();
       for (Tag t : partTags) {
           String partitionString = (String) t.getValue();
           Partition partition = (Partition) createMetastoreObject(partitionString, Partition.class);
           partList.add(partition);
       }
       return partList.get(0);
   }

   private <T> Object createMetastoreObject(String dbJson, Class<T> klass) {
       Gson gson = new Gson();
       return gson.fromJson(dbJson, klass);
   }
}
