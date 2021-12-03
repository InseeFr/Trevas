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
import java.io.File;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkSQLTest {

    private SparkSession spark;
    private ScriptEngine engine;
    private File databaseFile;

    @BeforeEach
    public void setUp() throws SQLException, IOException {
        databaseFile = File.createTempFile("vtl-test", "h2");
        databaseFile.deleteOnExit();
        var connection = DriverManager.getConnection("jdbc:h2:" + databaseFile);
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
        statement.executeUpdate("insert into ds1 values (2, 'string2', 5.2, 'false')");
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
    public void testReadSql() throws ScriptException {

        // See https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
        var ds1 = spark.read().format("jdbc")
                .option("url", "jdbc:h2:" + databaseFile)
                //.option("dbtable", "DS1")
                .option("query", "select * from DS1")
                .load();

        var bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.put("ds1", new SparkDataset(ds1, Map.of("id", Dataset.Role.IDENTIFIER)));

        engine.eval("ds2 := ds1[calc COL4 := if COL3 then COL2 + 1.0 else COL2 - 1.0];");

        var ds2 = (Dataset) bindings.get("ds2");

        assertThat(ds2.getDataAsMap()).containsExactly(
                Map.of("ID", 1L, "COL1", "string1", "COL2", 1.2D,
                        "COL3", true, "COL4", 2.2D),
                Map.of("ID", 2L, "COL1", "string2", "COL2", 5.2D,
                        "COL3", false, "COL4", 4.2D),
                Map.of("ID", 3L, "COL1", "string3", "COL2", 3.2D,
                        "COL3", true, "COL4", 4.2D),
                Map.of("ID", 4L, "COL1", "string4", "COL2", 4.2D,
                        "COL3", false, "COL4", 3.2D)
        );
    }
}
