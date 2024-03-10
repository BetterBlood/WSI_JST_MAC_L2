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
        var dbVisualizationQuery = """
                match (sick : Person {healthstatus : 'Sick'})-[v:VISITS]->(p:Place)
                Where sick.confirmedtime < v.starttime
                with sick, count(distinct p) as nbPlaces
                where nbPlaces > 10
                return sick.name as sickName, nbPlaces order by nbPlaces desc
                """;
        try(var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> sociallyCareful() {
        var dbVisualizationQuery = """
                match (carefully : Person {healthstatus : 'Sick'})
                where not exists {
                    (carefully) - [v:Visit] -> (p:Place {type : 'Bar'})
                    where carefully.confirmedtime < v.starttime
                }
                return carefully.name as sickName
                """;
        try(var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
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
        var dbVisualizationQuery =
                "match (sick : Person {healthstatus :'Sick'}) " +
                        "where sick.name in $names " +
                        "return sick.name as sickName";
        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery,
                                     Values.parameters("names", names)
            );
            return result.list();
        }
    }
}
