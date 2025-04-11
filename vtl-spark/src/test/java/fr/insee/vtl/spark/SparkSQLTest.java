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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        statement.executeUpdate("create table if not exists ds1 (" +
                "  id integer," +
                "  colChar char(7), " +
                "  colVarchar varchar, " +
                "  colInteger int," +
                "  colInteger4 int4," +
                "  colFloat float," +
                "  colFloat8 float8," +
                "  colBoolean boolean," +
                "  colNumeric numeric(20,2)," +
                "  colBigint int8," +
                "  colTimestamp timestamp," +
                "  colDate date," +
                "  primary key (id)" +
                ")");

        statement.executeUpdate("delete from ds1");
        statement.executeUpdate("insert into ds1 values (1, 'string1', 'string1', 1, 1, 1.2, 1.2, 'true', 1.2, 100, '2001-01-01T00:00:00Z', '2001-01-01')");
        statement.executeUpdate("insert into ds1 values (2, 'string2', 'string2', 2, 2, 5.2, 5.2, 'false', 5.2, 200, '2002-01-01T00:00:00Z', '2002-01-01')");
        statement.executeUpdate("insert into ds1 values (3, 'string3', 'string3', 3, 3, 3.2, 3.2, 'true', 3.2, 300, '2003-01-01T00:00:00Z', '2003-01-01')");
        statement.executeUpdate("insert into ds1 values (4, 'string4', 'string4', 4, 4, 4.2, 4.2, 'false', 4.2, 400, '2004-01-01T00:00:00Z', '2004-01-01')");

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
                "col5 := round(COLFLOAT - 0.1, 1), " +
                "col6 := round(COLFLOAT8 - 0.0009, 4), " +
                "col7 := not(COLBOOLEAN), " +
                "col8 := round(COLNUMERIC + 0.0001, 4), " +
                "col9 := COLBIGINT + 9999, " +
                "col10 := cast(COLTIMESTAMP, string, \"YYYY\"), " +
                "col11 := cast(COLDATE, string, \"YYYY\") " +
                "]" +
                "[keep ID, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11];");

        var ds2 = (Dataset) bindings.get("ds2");
        List<List<Object>> roundedDs2 = ds2.getDataAsList().stream().map(line ->
                line.stream().map(element -> {
                    if (element instanceof Double) {
                        BigDecimal bd = new BigDecimal(Double.toString((Double) element));
                        bd = bd.setScale(10, RoundingMode.HALF_UP);
                        return bd.doubleValue();
                    }
                    return element;
                }).collect(Collectors.toList())
        ).collect(Collectors.toList());

        assertThat(roundedDs2).containsExactlyInAnyOrder(
                List.of(1L, "string1 ok", "string1 ok", 2L, 100L, 1.1D, 1.1991D, false, 1.2001D, 10099L, "2001", "2001"),
                List.of(2L, "string2 ok", "string2 ok", 3L, 200L, 5.1D, 5.1991D, true, 5.2001D, 10199L, "2002", "2002"),
                List.of(3L, "string3 ok", "string3 ok", 4L, 300L, 3.1D, 3.1991D, false, 3.2001D, 10299L, "2003", "2003"),
                List.of(4L, "string4 ok", "string4 ok", 5L, 400L, 4.1D, 4.1991D, true, 4.2001D, 10399L, "2004", "2004")
        );
    }
}
