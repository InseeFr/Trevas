package fr.insee.vtl.engine.visitors;

//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructType;

import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.DatasetWrapper;
import fr.insee.vtl.model.InMemoryDatasetWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class ClauseVisitorTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");


    }

    @Test
    public void testFilterClause() throws ScriptException {

//        SparkSession session = SparkSession.builder().master("local").appName("VTL").getOrCreate();
//
//        StructType structure = DataTypes.createStructType(List.of(
//                DataTypes.createStructField("name", DataTypes.StringType, true),
//                DataTypes.createStructField("age", DataTypes.LongType, true)
//        ));
//
//        List<Row> data = List.of(
//                RowFactory.create("Nico", 25),
//                RowFactory.create("Hadrien", 30)
//        );
//
//        Dataset<Row> dataset = session.createDataFrame(data, structure);
//        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        InMemoryDatasetWrapper data = new InMemoryDatasetWrapper(List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 10L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L)
        ));

        DatasetExpression dataset = new DatasetExpression() {

            @Override
            public Set<String> getColumns() {
                return Set.of("name", "age", "weight");
            }

            @Override
            public Class<?> getType(String col) {
                return Map.of("name", String.class, "age", Long.class, "weight", Long.class).get(col);
            }

            @Override
            public int getIndex(String col) {
                return Map.of("name", 0, "age", 1, "weight", 2).get(col);
            }

            @Override
            public DatasetWrapper resolve(Map<String, Object> context) {
                return data;
            }
        };

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[filter age = weight];");

        assertThat(engine.getContext().getAttribute("ds")).isEqualTo(List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 10L)
        ));

    }
}