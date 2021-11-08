package fr.insee.vtl.spark;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class SparkSQLTest {


    private SparkSession spark;
    private ScriptEngine engine;

    @BeforeEach
    void setUp() throws SQLException {
        //connection = DriverManager.getConnection("jdbc:sqlite::memory:?cache=shared");
        //Connection connection = DriverManager.getConnection("jdbc:h2:mem:database1");
        var connection = DriverManager.getConnection("jdbc:h2:~/test");
        var statement = connection.createStatement();
        statement.executeUpdate("" +
                                "create table if not exists ds1 (" +
                                "  id integer," +
                                "  col1 varchar, " +
                                "  col2 float," +
                                "  col3 boolean," +
                                "  primary key (id)" +
                                ")" +
                                "");

        statement.executeUpdate("delete from ds1");
        statement.executeUpdate("insert into ds1 values (1, 'string1', 1.2, 'true')");
        statement.executeUpdate("insert into ds1 values (2, 'string2', 2.2, 'false')");
        statement.executeUpdate("insert into ds1 values (3, 'string3', 3.2, 'true')");
        statement.executeUpdate("insert into ds1 values (4, 'string4', 4.2, 'false')");

        spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");
        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    }

    @Test
    void testReadSql() throws ScriptException {

        // See https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
        var ds1 = spark.read().format("jdbc")
                .option("url", "jdbc:h2:~/test")
                //.option("dbtable", "DS1")
                .option("query", "select * from DS1")
                .load();

        var bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.put("ds1", new SparkDataset(ds1, Map.of("id", Dataset.Role.IDENTIFIER)));

        engine.eval("ds2 := ds1[calc COL4 := if COL3 then COL2 + 1.0 else COL2 - 1.0];");

        var ds2 = (Dataset) bindings.get("ds2");

        System.out.println(ds2.getDataAsMap());
    }

    @Test
    void testReadInMemory() throws SQLException, ScriptException {

        var connection = DriverManager.getConnection("jdbc:h2:~/test");
        var ds1 = new SqlDataset(connection, "select * from ds1");

        var bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.put("ds1", ds1);

        engine.eval("ds2 := ds1[calc COL4 := if COL3 then COL2 + 1.0 else COL2 - 1.0];");

        var ds2 = (Dataset) bindings.get("ds2");

        System.out.println(ds2.getDataAsMap());

    }

    private class SqlDataset implements Dataset {
        public SqlDataset(Connection connection, String s) {
        }

        @Override
        public List<DataPoint> getDataPoints() {
            return null;
        }

        @Override
        public DataStructure getDataStructure() {
            return null;
        }
    }
}
