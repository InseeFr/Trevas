package fr.insee.vtl.spark;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.InMemoryDataset;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static fr.insee.vtl.model.Structured.Component;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkDatasetTest {

    private SparkSession spark;
    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");
        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    }

    @Test
    public void testFilters() {
        StructType schema = DataTypes.createStructType(List.of(
                DataTypes.createStructField("string", DataTypes.StringType, false),
                DataTypes.createStructField("integer", DataTypes.LongType, false),
                DataTypes.createStructField("boolean", DataTypes.BooleanType, false),
                DataTypes.createStructField("float", DataTypes.DoubleType, false)
        ));

        Dataset<Row> dataFrame = spark.createDataFrame(List.of(
                RowFactory.create("string", 1L, true, 1.5D)
        ), schema);

        dataFrame.filter("integer > 0").collectAsList();
        dataFrame.filter((FilterFunction<Row>) row -> false).collectAsList();
        dataFrame.selectExpr("integer + 2 as newInteger").collectAsList();

    }

    @Test
    void testVtlCanReadSpark() {

        StructType schema = DataTypes.createStructType(List.of(
                DataTypes.createStructField("string", DataTypes.StringType, false),
                DataTypes.createStructField("integer", DataTypes.LongType, false),
                DataTypes.createStructField("boolean", DataTypes.BooleanType, false),
                DataTypes.createStructField("float", DataTypes.DoubleType, false)
        ));

        Dataset<Row> dataFrame = spark.createDataFrame(List.of(
                RowFactory.create("string", 1L, true, 1.5D)
        ), schema);


        fr.insee.vtl.model.Dataset sparkDataset = new SparkDataset(dataFrame);

        fr.insee.vtl.model.Dataset expectedDataset = new InMemoryDataset(
                List.of(List.of("string", 1L, true, 1.5D)),
                List.of(
                        new Component("string", String.class, fr.insee.vtl.model.Dataset.Role.MEASURE),
                        new Component("integer", Long.class, fr.insee.vtl.model.Dataset.Role.MEASURE),
                        new Component("boolean", Boolean.class, fr.insee.vtl.model.Dataset.Role.MEASURE),
                        new Component("float", Double.class, fr.insee.vtl.model.Dataset.Role.MEASURE)
                )
        );

        assertThat(sparkDataset.getDataStructure()).isEqualTo(expectedDataset.getDataStructure());
        assertThat(sparkDataset.getDataPoints()).isEqualTo(expectedDataset.getDataPoints());

    }

    @Test
    public void testParquetMetadataReading(@TempDir Path tmpDirectory) {
        SparkDataset sparkDataset = new SparkDataset(
                spark.read().parquet("src/main/resources/input_sample"),
                Map.of(
                        "year", fr.insee.vtl.model.Dataset.Role.IDENTIFIER,
                        "student_number", fr.insee.vtl.model.Dataset.Role.ATTRIBUTE
                )
        );
        assertTrue(sparkDataset.getDataStructure().get("year").isIdentifier());
        assertTrue(sparkDataset.getDataStructure().get("student_number").isAttribute());

        // Write the file as parquet and read again.
        sparkDataset.getSparkDataset().write().mode(SaveMode.Overwrite).parquet(tmpDirectory.toString());
        SparkDataset readSparkDataset = new SparkDataset(spark.read().parquet(tmpDirectory.toString()));

        assertTrue(readSparkDataset.getDataStructure().get("year").isIdentifier());
        assertTrue(readSparkDataset.getDataStructure().get("student_number").isAttribute());
    }

    @Test
    public void testParquetMetadataWriting(@TempDir Path tmpDirectory) throws ScriptException {
        SparkDataset datasetWithoutMetadata = new SparkDataset(spark.read().parquet("src/main/resources/input_sample"));
        ScriptContext context = engine.getContext();
        context.setAttribute("ds", datasetWithoutMetadata, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds1 := ds[calc identifier school_id := school_id, attribute year := year];");

        SparkDataset dsWithRoles = (SparkDataset) engine.getContext().getAttribute("ds1");
        dsWithRoles.getSparkDataset()
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(tmpDirectory.toString());

        SparkDataset dsWithMetadata = new SparkDataset(spark.read().parquet(tmpDirectory.toString()));

        assertTrue(dsWithMetadata.getDataStructure().get("school_id").isIdentifier());
        assertTrue(dsWithMetadata.getDataStructure().get("year").isAttribute());

        context.setAttribute("ds2", dsWithMetadata, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds3 := ds2[calc attribute school_id := school_id, identifier year := year];");

        SparkDataset dsWithMetadataAndRoles = (SparkDataset) engine.getContext().getAttribute("ds3");

        assertTrue(dsWithMetadataAndRoles.getDataStructure().get("school_id").isAttribute());
        assertTrue(dsWithMetadataAndRoles.getDataStructure().get("year").isIdentifier());
    }
}
