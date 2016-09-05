package edu.berkeley.ground.api.models;

import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.Optional;
import java.util.List;

public abstract class NodeFactory {
    public abstract Node create(String name) throws GroundException;

    public abstract Node retrieveFromDatabase(String name) throws GroundException;

    public abstract void update(GroundDBConnection connection, String itemId, String childId, Optional<String> parent) throws GroundException;

    public abstract List<String> getLeaves(String name) throws GroundException;

    protected static Node construct(String id, String name) {
        return new Node(id, name);
    }
}
