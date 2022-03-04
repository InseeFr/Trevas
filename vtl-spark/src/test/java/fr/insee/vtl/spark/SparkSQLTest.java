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
                "  colChar char, " +
                "  colVarchar varchar, " +
                "  colInteger int," +
                "  colInteger4 int4," +
                "  colFloat float," +
                "  colFloat8 float8," +
                "  colBoolean boolean," +
                "  colNumeric numeric(20,2)," +
                "  colBigint int8," +
                "  primary key (id)" +
                ")");

        statement.executeUpdate("delete from ds1");
        statement.executeUpdate("insert into ds1 values (1, 'string1', 'string1', 1, 1, 1.2, 1.2, 'true', 1.2, 100)");
        statement.executeUpdate("insert into ds1 values (2, 'string2', 'string2', 2, 2, 5.2, 5.2, 'false', 5.2, 200)");
        statement.executeUpdate("insert into ds1 values (3, 'string3', 'string3', 3, 3, 3.2, 3.2, 'true', 3.2, 300)");
        statement.executeUpdate("insert into ds1 values (4, 'string4', 'string4', 4, 4, 4.2, 1.2, 'false', 4.2, 400)");

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

        engine.eval("ds2 := ds1[calc " +
                "col1 := COLCHAR || \" ok\", " +
                "col2 := COLVARCHAR || \" ok\", " +
                "col3 := COLINTEGER + 1, " +
                "col4 := COLINTEGER4 * 100, " +
                "col5 := COLFLOAT - 0.1, " +
                "col6 := COLFLOAT8 - 0.0009, " +
                "col7 := not(COLBOOLEAN), " +
                "col8 := COLNUMERIC + 0.0001, " +
                "col9 := COLBIGINT + 9999 " +
                "];");

        var ds2 = (Dataset) bindings.get("ds2");

        assertThat(ds2.getDataAsMap()).containsExactly(
                Map.of("ID", 1L, "COL1", "string1 ok", "COL2", "string1 ok",
                        "COL3", 2L, "COL4", 100L, "COL5", 1.1D,
                        "COL6", 1.1991D, "COL7", false,
                        "COL8", 1.2001D, "COL9", 10099L),
                Map.of("ID", 2L, "COL1", "string2 ok", "COL2", "string2 ok",
                        "COL3", 3L, "COL4", 200L, "COL5", 5.1D,
                        "COL6", 5.1991D, "COL7", true,
                        "COL8", 5.2001D, "COL9", 10199L),
                Map.of("ID", 3L, "COL1", "string3 ok", "COL2", "string3 ok",
                        "COL3", 4L, "COL4", 300L, "COL5", 3.1D,
                        "COL6", 3.1991D, "COL7", false,
                        "COL8", 3.2001D, "COL9", 10299L),
                Map.of("ID", 5L, "COL1", "string4 ok", "COL2", "string4 ok",
                        "COL3", 4L, "COL4", 400L, "COL5", 4.1D,
                        "COL6", 4.1991D, "COL7", true,
                        "COL8", 4.2001D, "COL9", 10399L)
        );
    }
}
