package edu.berkeley.ground.api.models.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.GroundException;
import static org.junit.Assert.*;

public class Neo4jRichVersionFactoryTest extends Neo4jTest {

    public Neo4jRichVersionFactoryTest() throws GroundException {
        super();
    }

    @Test
    public void testReference() throws GroundException {
        Neo4jConnection connection = null;
        try {
            connection = super.neo4jClient.getConnection();

            /* Create a NodeVersion because Neo4j's rich version factory looks for an existing
             * version with this id */
            String testNodeId = super.factories.getNodeFactory().create("testNode").getId();
            String id = super.createNodeVersion(testNodeId);

            String testReference = "http://www.google.com";
            Map<String, String> parameters = new HashMap<>();
            parameters.put("http", "GET");

            super.richVersionFactory.insertIntoDatabase(connection, id, new HashMap<>(), null,
                    testReference, parameters);

            RichVersion retrieved = super.richVersionFactory.retrieveFromDatabase(connection, id);

            assertEquals(id, retrieved.getId());
            assertEquals(testReference, retrieved.getReference());

            Map<String, String> retrievedParams = retrieved.getParameters();
            for (String key : parameters.keySet()) {
                assert(retrievedParams).containsKey(key);
                assertEquals(parameters.get(key), retrievedParams.get(key));
            }
        } finally {
            connection.abort();
        }
    }

    @Test
    public void testTags() throws GroundException {
        Neo4jConnection connection = null;
        try {
            connection = super.neo4jClient.getConnection();

            /* Create a NodeVersion because Neo4j's rich version factory looks for an existing
             * version with this id */
            String testNodeId = super.factories.getNodeFactory().create("testNode").getId();
            String id = super.createNodeVersion(testNodeId);

            Map<String, Tag> tags = new HashMap<>();
            tags.put("justkey", new Tag(null, "justkey", null, null));
            tags.put("withintvalue", new Tag(null, "withintvalue", 1, GroundType.INTEGER));
            tags.put("withstringvalue", new Tag(null, "withstringvalue", "1", GroundType.STRING));
            tags.put("withboolvalue", new Tag(null, "withboolvalue", true, GroundType.BOOLEAN));

            super.richVersionFactory.insertIntoDatabase(connection, id, tags, null,
                    null, new HashMap<>());

            RichVersion retrieved = super.richVersionFactory.retrieveFromDatabase(connection, id);

            assertEquals(id, retrieved.getId());
            assertEquals(tags.size(), retrieved.getTags().size());

            Map<String, Tag> retrievedTags = retrieved.getTags();
            for (String key : tags.keySet()) {
                assert(retrievedTags).containsKey(key);
                assertEquals(tags.get(key), retrievedTags.get(key));
                assertEquals(retrieved.getId(), retrievedTags.get(key).getVersionId());
            }
        } finally {
            connection.abort();
        }
    }

    @Test
    public void testStructureVersionConformation() throws GroundException {
        Neo4jConnection connection = null;
        try {
            connection = super.neo4jClient.getConnection();

            /* Create a NodeVersion because Neo4j's rich version factory looks for an existing
             * version with this id */
            String testNodeId = super.factories.getNodeFactory().create("testNode").getId();
            String id = super.createNodeVersion(testNodeId);

            String structureName = "testStructure";
            String structureId =  super.factories.getStructureFactory().create(structureName).getId();

            Map<String, GroundType> structureVersionAttributes = new HashMap<>();
            structureVersionAttributes.put("intfield", GroundType.INTEGER);
            structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
            structureVersionAttributes.put("strfield", GroundType.STRING);

            String structureVersionId = super.factories.getStructureVersionFactory().create(
                    structureId, structureVersionAttributes, new ArrayList<>()).getId();

            Map<String, Tag> tags = new HashMap<>();
            tags.put("intfield", new Tag(null, "intfield", 1, GroundType.INTEGER));
            tags.put("strfield", new Tag(null, "strfield", "1", GroundType.STRING));
            tags.put("boolfield", new Tag(null, "boolfield", true, GroundType.BOOLEAN));

            super.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, null,
                    new HashMap<>());

            RichVersion retrieved = super.richVersionFactory.retrieveFromDatabase(connection, id);
            assertEquals(retrieved.getStructureVersionId(), structureVersionId);
        } finally {
            connection.abort();
        }
    }

    @Test(expected = GroundException.class)
    public void testStructureVersionFails() throws GroundException {
        String structureVersionId = null;
        Neo4jConnection connection = null;

        try {
        /* Create a NodeVersion because Neo4j's rich version factory looks for an existing
         * version with this id */
            String testNodeId = super.factories.getNodeFactory().create("testNode").getId();
            String id = super.createNodeVersion(testNodeId);

            // none of these operations should fail
            try {
                connection = super.neo4jClient.getConnection();

                String structureName = "testStructure";
                String structureId = super.factories.getStructureFactory().create(structureName).getId();

                Map<String, GroundType> structureVersionAttributes = new HashMap<>();
                structureVersionAttributes.put("intfield", GroundType.INTEGER);
                structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
                structureVersionAttributes.put("strfield", GroundType.STRING);

                structureVersionId = super.factories.getStructureVersionFactory().create(
                        structureId, structureVersionAttributes, new ArrayList<>()).getId();
            } catch (GroundException ge) {
                fail(ge.getMessage());
            }

            Map<String, Tag> tags = new HashMap<>();
            tags.put("intfield", new Tag(null, "intfield", 1, GroundType.INTEGER));
            tags.put("intfield", new Tag(null, "strfield", "1", GroundType.STRING));
            tags.put("intfield", new Tag(null, "boolfield", true, GroundType.BOOLEAN));

            // this should fail
            super.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, null,
                    new HashMap<>());
        } finally {
            connection.abort();
        }
    }
}
