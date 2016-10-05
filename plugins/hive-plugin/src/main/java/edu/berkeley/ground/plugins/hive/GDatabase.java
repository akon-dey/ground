package edu.berkeley.ground.plugins.hive;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.GroundStore.EntityState;

public class GDatabase {
    static final private Logger LOG = LoggerFactory.getLogger(GDatabase.class.getName());

    static final String DATABASE_NODE = "_DATABASE";

    static final String DATABASE_TABLE_EDGE = "_DATABASE_TABLE";

    private static final List<String> EMPTY_PARENT_LIST = new ArrayList<String>();

    private GroundReadWrite ground = null;
    private GTable table = null;

    public GDatabase(GroundReadWrite ground) {
        this.ground = ground;
        this.table = new GTable(ground);
    }

    public Node getNode(String dbName) throws GroundException {
        try {
            LOG.debug("Fetching database node: " + dbName);
            return ground.getNodeFactory().retrieveFromDatabase(dbName);
        } catch (GroundException ge1) {
            LOG.debug("Not found - Creating databsae node: " + dbName);

            Node node = ground.getNodeFactory().create(dbName);
            Structure nodeStruct = ground.getStructureFactory().create(node.getName());

            return node;
        }
    }

    public Structure getNodeStructure(String dbName) throws GroundException {
        try {
            Node node = this.getNode(dbName);
            return ground.getStructureFactory().retrieveFromDatabase(dbName);
        } catch (GroundException e) {
            LOG.error("Unable to fetch database node structure");
            throw e;
        }
    }

    public Edge getEdge(NodeVersion nodeVersion) throws GroundException {
        String edgeId = nodeVersion.getNodeId();
        try {
            LOG.debug("Fetching database table edge: " + edgeId);
            return ground.getEdgeFactory().retrieveFromDatabase(edgeId);
        } catch (GroundException e) {
            LOG.debug("Not found - Creating database table edge: " + edgeId);
            Edge edge = ground.getEdgeFactory().create(edgeId);
            Structure edgeStruct = ground.getStructureFactory().create(edge.getName());
            return edge;
        }
    }

    public Structure getEdgeStructure(NodeVersion nodeVersion) throws GroundException {
        try {
            LOG.debug("Fetching database table edge structure: " + nodeVersion.getNodeId());
            Edge edge = this.getEdge(nodeVersion);
            return ground.getStructureFactory().retrieveFromDatabase(edge.getName());
        } catch (GroundException e) {
            LOG.debug("Not found - database table edge structure: " + nodeVersion.getNodeId());
            throw e;
        }
    }

    public Database fromJSON(String json) {
        Gson gson = new Gson();
        return (Database) gson.fromJson(json, Database.class);
    }

    public String toJSON(Database db) {
        Gson gson = new Gson();
        return gson.toJson(db);
    }

    public Database getDatabase(String dbName) throws NoSuchObjectException {
        try {
            List<String> versions = ground.getNodeFactory().getLeaves(dbName);
            if (versions.isEmpty()) {
                throw new GroundException("Database node not found: " + dbName);
            }

            NodeVersion latestVersion = ground.getNodeVersionFactory().retrieveFromDatabase(versions.get(0));
            Map<String, Tag> dbTag = latestVersion.getTags();

            return this.fromJSON((String) dbTag.get(dbName).getValue());
        } catch (GroundException e) {
            throw new NoSuchObjectException(e.getMessage());
        }
    }

    public NodeVersion createDatabase(Database db) throws InvalidObjectException, MetaException {
        if (db == null) {
            throw new InvalidObjectException("Database object passed is null");
        }
        try {
            String dbName = db.getName();
            Node dbNode = this.getNode(dbName);
            Structure dbStruct = this.getNodeStructure(dbName);

            Tag dbTag = new Tag("1.0.0", dbName, toJSON(db), GroundType.STRING);

            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            structVersionAttribs.put(dbName, GroundType.STRING);

            StructureVersion sv = ground.getStructureVersionFactory().create(dbStruct.getId(), structVersionAttribs, EMPTY_PARENT_LIST);

            String reference = db.getLocationUri();
            HashMap<String, Tag> tags = new HashMap<>();
            tags.put(dbName, dbTag);

            Map<String, String> dbParamMap = db.getParameters();
            if (dbParamMap == null) {
                dbParamMap = new HashMap<String, String>();
            }
            Map<String, String> parameters = dbParamMap;

            List<String> parent = new ArrayList<String>();
            List<String> versions = ground.getNodeFactory().getLeaves(dbName);
            if (!versions.isEmpty()) {
                parent.add(versions.get(0));
            }

            NodeVersion dbNodeVersion = ground.getNodeVersionFactory().create(tags, sv.getId(), reference, parameters, dbNode.getId(), parent);

            return dbNodeVersion;
        } catch (GroundException e) {
            LOG.error("Failure to create a database node: " + db.getName());
            throw new MetaException(e.getMessage());
        }
    }

    // Table related functions
    public NodeVersion createTable(Table table) throws InvalidObjectException, MetaException {
        try {
            String dbName = table.getDbName();
            NodeVersion tableNodeVersion = this.table.createTable(table);
            Database prevDb = this.getDatabase(dbName);

            List<String> versions = ground.getNodeFactory().getLeaves(dbName);

            NodeVersion dbNodeVersion = this.createDatabase(prevDb);
            String dbNodeVersionId = dbNodeVersion.getId();

            Edge edge = this.getEdge(tableNodeVersion);
            Structure structure = this.getEdgeStructure(tableNodeVersion);
            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            for (String key: tableNodeVersion.getTags().keySet()) {
                structVersionAttribs.put(key, GroundType.STRING);
            }
            StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                    new ArrayList<>());

            ground.getEdgeVersionFactory().create(tableNodeVersion.getTags(), sv.getId(), tableNodeVersion.getReference(), tableNodeVersion.getParameters(),
                    edge.getId(), dbNodeVersionId, tableNodeVersion.getId(), new ArrayList<String>());

            if (!versions.isEmpty()) {
                if (versions.size() != 0) {
                    String prevVersionId = versions.get(0);
                    List<String> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
                    for (String nodeId : nodeIds) {
                        NodeVersion oldNV = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);
                        edge = this.getEdge(oldNV);

                        structVersionAttribs = new HashMap<>();
                        for (String key: oldNV.getTags().keySet()) {
                            structVersionAttribs.put(key, GroundType.STRING);
                        }

                        // create an edge version for a dbname
                        sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                                new ArrayList<>());
                        ground.getEdgeVersionFactory().create(oldNV.getTags(), sv.getId(), oldNV.getReference(),
                                oldNV.getParameters(), edge.getId(), dbNodeVersionId, oldNV.getId(),
                                new ArrayList<String>());
                    }
                }
            }

            return dbNodeVersion;
        } catch (GroundException ex) {
            throw new MetaException(ex.getMessage());
        } catch (NoSuchObjectException ex) {
            throw new MetaException(ex.getMessage());
        }
    }

    public NodeVersion dropTable(String dbName, String tableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        try {
            boolean found = false;
            List<String> versions = ground.getNodeFactory().getLeaves(dbName);

            if (versions.isEmpty()) {
                LOG.error("Could not find table to drop named {}", tableName);
                return null;
            } else {
                String prevVersionId = versions.get(0);
                List<String> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");

                if (nodeIds.size() == 0) {
                    LOG.error("Failed to drop table {}", dbName);
                    return null;
                }
                Database db = this.getDatabase(dbName);
                NodeVersion dbNodeVersion = this.createDatabase(db);
                String dbVersionId = dbNodeVersion.getId();
                String tableNodeId = "Nodes." + tableName;

                for (String nodeId : nodeIds) {
                    NodeVersion oldNV = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);

                    if (!oldNV.getNodeId().equals(tableNodeId)) {
                        Edge edge = this.getEdge(oldNV);
                        Structure structure = this.getEdgeStructure(oldNV);

                        LOG.error("Found edge with name {}", oldNV.getNodeId());

                        Map<String, GroundType> structVersionAttribs = new HashMap<>();
                        for (String key: oldNV.getTags().keySet()) {
                            structVersionAttribs.put(key, GroundType.STRING);
                        }
                        // create an edge for each table other than the one
                        // being deleted
                        StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
                                structVersionAttribs, new ArrayList<>());
                        ground.getEdgeVersionFactory().create(oldNV.getTags(), sv.getId(), oldNV.getReference(),
                                oldNV.getParameters(), edge.getId(), dbVersionId, oldNV.getId(),
                                new ArrayList<String>());
                    }
                }
                return dbNodeVersion;
            }
        } catch (GroundException ex) {
            LOG.error("Failed to drop table {}", tableName);
            throw new MetaException("Failed to drop table: " + ex.getMessage());
        }
    }

    public Table getTable(String dbName, String tableName) throws MetaException {
        return this.table.getTable(dbName, tableName);
    }

    public List<String> getTables(String dbName, String pattern) throws MetaException {
        return this.table.getTables(dbName, pattern);
    }
}