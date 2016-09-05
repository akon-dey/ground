package edu.berkeley.ground.api.versions.postgres;

import edu.berkeley.ground.api.versions.ItemFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient.PostgresConnection;
import edu.berkeley.ground.exceptions.GroundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PostgresItemFactory extends ItemFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresItemFactory.class);

    private PostgresVersionHistoryDAGFactory versionHistoryDAGFactory;

    public PostgresItemFactory(PostgresVersionHistoryDAGFactory versionHistoryDAGFactory) {
        this.versionHistoryDAGFactory = versionHistoryDAGFactory;
    }

    public void insertIntoDatabase(GroundDBConnection connectionPointer, String id) throws GroundException {
        PostgresConnection connection = (PostgresConnection) connectionPointer;

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));

        connection.insert("Items", insertions);
    }

    public void update(GroundDBConnection connectionPointer, String itemId, String childId, Optional<String> parent) throws GroundException {
        String parentId;

        // If a parent is specified, great. If it's not specified and there is only one leaf, great.
        // If it's not specified, there are either 0 or > 1 leaves, then make it a child of EMPTY.
        // Eventually, there should be a specification about empty-child?
        if (parent.isPresent()) {
            parentId = parent.get();
        } else {
            List<String> leaves = this.getLeaves(connectionPointer, itemId);
            if (leaves.size() == 1) {
                parentId = leaves.get(0);
            } else {
                parentId = "EMPTY";
            }
        }

        VersionHistoryDAG dag;
        try {
            dag = this.versionHistoryDAGFactory.retrieveFromDatabase(connectionPointer, itemId);
        } catch (GroundException e) {
            if (!e.getMessage().contains("No results found for query:")) {
                throw e;
            }

            dag = this.versionHistoryDAGFactory.create(itemId);
        }

        if (parent.isPresent() && !dag.checkItemInDag(parentId)) {
            String errorString = "Parent " + parent + " is not in Item " + itemId + ".";

            LOGGER.error(errorString);
            throw new GroundException(errorString);
        }

        this.versionHistoryDAGFactory.addEdge(connectionPointer, dag, parentId, childId, itemId);
    }

    public List<String> getLeaves(GroundDBConnection connection, String itemId) throws GroundException {
        try {
            VersionHistoryDAG<?> dag = this.versionHistoryDAGFactory.retrieveFromDatabase(connection, itemId);

            return dag.getLeaves();
        } catch (GroundException e) {
            if (!e.getMessage().contains("No results found for query:")) {
                throw e;
            }

            return new ArrayList<>();
        }
    }

}
