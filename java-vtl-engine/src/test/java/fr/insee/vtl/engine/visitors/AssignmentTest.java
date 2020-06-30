package fr.insee.vtl.engine.visitors;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.*;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class AssignmentTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testAssignment() throws ScriptException {
        Bindings bindings = engine.createBindings();
        engine.eval("a := 1234;", bindings);
        engine.eval("b := 1234.1234;", bindings);
        engine.eval("c := true;", bindings);
        engine.eval("d := false;", bindings);
        engine.eval("e := \"foo\";", bindings);
        engine.eval("f := null;", bindings);

        assertThat(bindings).containsAllEntriesOf(Map.of(
                "a", 1234L,
                "b", 1234.1234,
                "c", true,
                "d", false,
                "e", "foo"
        ));

        assertThat(bindings.get("f")).isNull();
    }

    @Test
    void testDatasetAssignment() {
        SparkSession session = SparkSession.builder().master("local").appName("VTL").getOrCreate();

        List<Row> data = List.of(
                RowFactory.create("a string")
        );

        StructType structure = DataTypes.createStructType(
                List.of(
                        DataTypes.createStructField("aString", DataTypes.StringType, true)
                )
        );

        Dataset<Row> dataset = session.createDataFrame(data, structure);
        System.out.println(dataset.count());
        dataset.show();
    }
}