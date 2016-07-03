package edu.berkeley.ground.plugins.hive;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.GraphFactory;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.api.models.TagFactory;
import edu.berkeley.ground.api.models.cassandra.CassandraNodeFactory;
import edu.berkeley.ground.api.models.cassandra.CassandraNodeVersionFactory;

public class GroundReadWrite {

    static final private Logger LOG = LoggerFactory.getLogger(GroundReadWrite.class.getName());

    static final String GRAPHFACTORY_CLASS = "ground.graph.factory";

    static final String NODEFACTORY_CLASS = "ground.node.factory";

    static final String EDGEFACTORY_CLASS = "ground.edge.factory";

    static final String NO_CACHE_CONF = "no.use.cache";
    private DBClient dbClient;
    private GraphFactory graphFactory;
    private NodeVersionFactory nodeVersionFactory;
    private EdgeVersionFactory edgeVersionFactory;
    private TagFactory tagFactory;
    private String factoryType;

    @VisibleForTesting final static String TEST_CONN = "test_connection";
    private static GroundDBConnection testConn;

    private static Configuration staticConf = null;
    private final Configuration conf;

    private GroundDBConnection conn;
    

    private static ThreadLocal<GroundReadWrite> self = new ThreadLocal<GroundReadWrite>() {
        @Override
        protected GroundReadWrite initialValue() {
            if (staticConf == null) {
                throw new RuntimeException("Must set conf object before getting an instance");
            }
            return new GroundReadWrite(staticConf);
        }
    };

    /**
     * Set the configuration for all HBaseReadWrite instances.
     * @param configuration Configuration object
     */
    public static synchronized void setConf(Configuration configuration) {
      if (staticConf == null) {
        staticConf = configuration;
      } else {
        LOG.info("Attempt to set conf when it has already been set.");
      }
    }

    /**
     * Get the instance of GroundReadWrite for the current thread.
     */
    static GroundReadWrite getInstance() {
        if (staticConf == null) {
            throw new RuntimeException("Must set conf object before getting an instance");
        }
        return self.get();
    }

    private GroundReadWrite(Configuration configuration) {
        conf = configuration;
        try {
            String clientClass = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER);
            String graphFactoryType = conf.get(GRAPHFACTORY_CLASS);
            String nodeFactoryType = conf.get(NODEFACTORY_CLASS);
            String edgeFactoryType = conf.get(EDGEFACTORY_CLASS);
            LOG.info("client cass is " + clientClass);
            if (TEST_CONN.equals(clientClass)) {
                setConn(testConn);
                LOG.info("Using test connection.");
                createTestInstances();
            } else {
                LOG.debug("Instantiating connection class " + clientClass);
                Object o = createInstance(clientClass);
                if (DBClient.class.isAssignableFrom(o.getClass())) {
                    dbClient = (DBClient) o;
                    setConn(dbClient.getConnection());
                } else {
                    throw new IOException(clientClass + " is not an instance of DBClient.");
                }
                o = createInstance(graphFactoryType);
                if (GraphFactory.class.isAssignableFrom(o.getClass())) {
                    graphFactory = (GraphFactory) o;
                }
                o = createInstance(nodeFactoryType);
                if (NodeVersionFactory.class.isAssignableFrom(o.getClass())) {
                    nodeVersionFactory = (NodeVersionFactory) o;
                }
                o = createInstance(edgeFactoryType);
                if (EdgeVersionFactory.class.isAssignableFrom(o.getClass())) {
                    edgeVersionFactory = (EdgeVersionFactory) o;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createTestInstances() {
        NodeFactory nf = new CassandraNodeFactory(null, null);
        CassandraClient client = new CassandraClient(factoryType, 0, factoryType, factoryType, factoryType);
        nodeVersionFactory = new CassandraNodeVersionFactory((CassandraNodeFactory) nf, null, client);
    }

    private Object createInstance(String clientClass)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> c = Class.forName(clientClass);
        Object o = c.newInstance();
        return o;
    }

    /**
     * Use this for unit testing only, so that a mock connection object can be passed in.
     * @param connection Mock connection objecct
     */
    @VisibleForTesting
    static void setTestConnection(GroundDBConnection connection) {
      testConn = connection;
    }

    public void close() throws IOException {
    }

    public void begin() {
        try {
            dbClient.getConnection();
        } catch (GroundDBException e) {
        }
    }

    public void commit() {
        try {
            dbClient.getConnection().commit();
        } catch (GroundDBException e) {
            throw new RuntimeException(e);
        }
    }

    public GraphFactory getGraphFactory() {
        return graphFactory;
    }

    public void setGraphFactory(GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    public NodeVersionFactory getNodeVersionFactory() {
        return nodeVersionFactory;
    }

    public void setNodeFactory(NodeVersionFactory nodeFactory) {
        this.nodeVersionFactory = nodeFactory;
    }

    public EdgeVersionFactory getEdgeVersionFactory() {
        return edgeVersionFactory;
    }

    public void setEdgeVersionFactory(EdgeVersionFactory edgeVersionFactory) {
        this.edgeVersionFactory = edgeVersionFactory;
    }

    public String getFactoryType() {
        return factoryType;
    }

    public void setFactoryType(String factoryType) {
        this.factoryType = factoryType;
    }

    public TagFactory getTagFactory() {
        return tagFactory;
    }

    public void setTagFactory(TagFactory tagFactory) {
        this.tagFactory = tagFactory;
    }

    public GroundDBConnection getConn() {
        return conn;
    }

    public void setConn(GroundDBConnection conn) {
        this.conn = conn;
    }
}