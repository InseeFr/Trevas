import fr.insee.vtl.model.Dataset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


class JDBCDatasetTest {

    private ScriptEngine engine;
    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        //connection = DriverManager.getConnection("jdbc:sqlite::memory:?cache=shared");
        //Connection connection = DriverManager.getConnection("jdbc:h2:mem:database1");
        connection = DriverManager.getConnection("jdbc:h2:~/test");
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

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");
    }

    @Test
    void testReadSql() throws ScriptException, SQLException {

        var statement = connection.createStatement();
        var jdbcDataset = new JDBCDataset(() -> {
            try {
                return statement.executeQuery("select * from ds1;");
            } catch (SQLException se) {
                throw new RuntimeException(se);
            }
        });

        var bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.put("ds1", jdbcDataset);

        engine.eval("ds2 := ds1[calc COL4 := if COL3 then COL2 + 1.0 else COL2 - 1.0];");

        var ds2 = (Dataset) bindings.get("ds2");

        System.out.println(ds2.getDataAsMap());
    }

}
