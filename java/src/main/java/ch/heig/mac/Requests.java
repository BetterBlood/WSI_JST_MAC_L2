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
                    (carefully) - [v : VISITS] -> (p : Place {type : 'Bar'})
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
        var dbVisualizationQuery = """
                MATCH (sick:Person {healthstatus:'Sick'})-[sickV:VISITS]->(:Place)<-[healthyV:VISITS]-(healthy:Person {healthstatus:'Healthy'})
                WHERE sickV.starttime > sick.confirmedtime AND healthyV.starttime > healthy.confirmedtime
                WITH sick, healthy,
                     duration.inSeconds(
                         apoc.coll.max([sickV.starttime, healthyV.starttime]),
                         apoc.coll.min([sickV.endtime, healthyV.endtime])
                     ) AS chevauchement
                WHERE datetime() + chevauchement >= datetime() + duration({hours:2})
                RETURN sick.name AS sickName,
                       COLLECT(healthy.name) AS peopleToInform
                """;
        try(var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
        /*
        * MATCH (healthy:Person {healthstatus:'Healthy'})
        * WHERE healthy.risk = 'high'
        * RETURN DISTINCT healthy.id AS highRiskId, healthy.name AS highRiskName
        * */
    }

    public List<Record> setHighRisk() {
        var dbVisualizationQuery = """
                MATCH (sick:Person {healthstatus:'Sick'})-[sickV:VISITS]->(:Place)<-[healthyV:VISITS]-(healthy:Person {healthstatus:'Healthy'})
                WHERE sickV.starttime > sick.confirmedtime AND healthyV.starttime > healthy.confirmedtime
                WITH sick, healthy,
                     duration.inSeconds(
                         apoc.coll.max([sickV.starttime, healthyV.starttime]),
                         apoc.coll.min([sickV.endtime, healthyV.endtime])
                     ) AS chevauchement
                WHERE datetime() + chevauchement >= datetime() + duration({hours:2})
                SET healthy.risk = 'high'
                RETURN DISTINCT healthy.id AS highRiskId, healthy.name AS highRiskName
                """;
        try(var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> healthyCompanionsOf(String name) {
        var dbVisualizationQuery = """
                        match (p : Person {name : $name}) - [ : VISITS *..3] - (c : Person {healthstatus : 'Healthy'})
                        where p <> c
                        return distinct c.name AS healthyName;
                        """;
        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery,
                                     Values.parameters("name", name)
            );
            return result.list();
        }
    }

    public Record topSickSite() {
        var dbVisualizationQuery = """
                match (sick : Person {healthstatus : 'Sick'}) - [v : VISITS] - (p : Place)
                where sick.confirmedtime < v.starttime
                return p.type as placeType, count(v) AS nbOfSickVisits
                order by nbOfSickVisits desc
                limit 1
                """;
        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.next();
        }
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
