package edu.berkeley.ground.api.models.gremlin;

import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.gremlin.GremlinItemFactory;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.GremlinClient;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.GroundException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class GremlinNodeFactory extends NodeFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinNodeFactory.class);
    private GremlinClient dbClient;
    private GremlinItemFactory itemFactory;

    public GremlinNodeFactory(GremlinItemFactory itemFactory, GremlinClient dbClient) {
        this.dbClient = dbClient;
        this.itemFactory = itemFactory;
    }

    public Node create(String name) throws GroundException {
        GremlinConnection connection = this.dbClient.getConnection();

        try {
            String uniqueId = "Nodes." + name;

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("name", Type.STRING, name));
            insertions.add(new DbDataContainer("id", Type.STRING, uniqueId));

            connection.addVertex("Node", insertions);

            connection.commit();
            LOGGER.info("Created node " + name + ".");

            return NodeFactory.construct(uniqueId, name);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public List<String> getLeaves(String name) throws GroundException {
        GremlinConnection connection = this.dbClient.getConnection();
        return this.itemFactory.getLeaves(connection, name);
    }

    public Node retrieveFromDatabase(String name) throws GroundException {
        GremlinConnection connection = this.dbClient.getConnection();

        try {
            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("name", Type.STRING, name));
            predicates.add(new DbDataContainer("label", Type.STRING, "Nodes"));

            Vertex vertex = connection.getVertex(predicates);

            String id = (String) vertex.property("id").value();

            connection.commit();
            LOGGER.info("Retrieved node " + name + ".");

            return NodeFactory.construct(id, name);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public void update(GroundDBConnection connection, String itemId, String childId, Optional<String> parent) throws GroundException {
        this.itemFactory.update(connection, "Nodes." + itemId, childId, parent);
    }
}
