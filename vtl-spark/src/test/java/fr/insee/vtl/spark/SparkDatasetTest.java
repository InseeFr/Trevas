package fr.insee.vtl.spark;

import fr.insee.vtl.model.Dataset.Component;
import fr.insee.vtl.model.InMemoryDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkDatasetTest {

    private SparkSession spark;

    @BeforeEach
    public void setUp() {
        spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();
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
}