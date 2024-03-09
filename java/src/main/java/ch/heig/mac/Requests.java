package ch.heig.mac;

import java.util.List;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;

public class Requests {
    private final Driver driver;

    public Requests(Driver driver) {
        this.driver = driver;
    }

    public List<String> getDbLabels() {
        var dbVisualizationQuery = """
                CALL db.labels
                """;

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list(t -> t.get("label").asString());
        }
    }

    public List<Record> possibleSpreaders() {
        var dbVisualizationQuery = """
                MATCH (sick:Person {healthstatus:'Sick'})-[sickV:VISITS]-(:Place)-[healthyV:VISITS]-(:Person {healthstatus:'Healthy'})
                WHERE healthyV.starttime > sickV.starttime
                  AND sickV.starttime > sick.confirmedtime
                RETURN DISTINCT sick.name AS sickName
                """;
        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> possibleSpreadCounts() {
        var dbVisualizationQuery = """
                MATCH (sick:Person {healthstatus:'Sick'})-[sickV:VISITS]-(:Place)-[healthyV:VISITS]-(healthy:Person {healthstatus:'Healthy'})
                WHERE healthyV.starttime > sickV.starttime
                  AND sickV.starttime > sick.confirmedtime
                RETURN DISTINCT sick.name AS sickName, COUNT(healthy) AS nbHealthy
                """;
        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> carelessPeople() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> sociallyCareful() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> peopleToInform() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> setHighRisk() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> healthyCompanionsOf(String name) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public Record topSickSite() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> sickFrom(List<String> names) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }
}
