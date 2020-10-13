package fr.insee.vtl.spark;

import fr.insee.vtl.model.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.*;

/**
 * A wrapper around a spark dataframe
 */
public class SparkDataset implements Dataset {

    private final org.apache.spark.sql.Dataset<Row> sparkDataset;
    private List<Component> components = null;

    public SparkDataset(org.apache.spark.sql.Dataset<Row> sparkDataset) {
        this.sparkDataset = sparkDataset;
    }

    public SparkDataset(Dataset vtlDataset, SparkSession spark) {

        List<StructField> schema = new ArrayList<>();
        for (Component component : vtlDataset.getDataStructure()) {
            schema.add(DataTypes.createStructField(
                    component.getName(),
                    fromVtlType(component.getType()),
                    false
            ));
        }

        List<Row> rows = vtlDataset.getDataPoints().stream().map(points ->
                RowFactory.create(points.toArray(new Object[]{}))
        ).collect(Collectors.toList());

        sparkDataset = spark.createDataFrame(rows, DataTypes.createStructType(schema));
    }

    public static Class<?> toVtlType(DataType dataType) {
        if (StringType.sameType(dataType)) {
            return String.class;
        } else if (IntegerType.sameType(dataType)) {
            return Long.class;
        } else if (LongType.sameType(dataType)) {
            return Long.class;
        } else if (FloatType.sameType(dataType)) {
            return Double.class;
        } else if (DoubleType.sameType(dataType)) {
            return Double.class;
        } else if (BooleanType.sameType(dataType)) {
            return Boolean.class;
        } else {
            throw new UnsupportedOperationException("unsuported type " + dataType);
        }
    }

    private DataType fromVtlType(Class<?> type) {
        if (String.class.equals(type)) {
            return StringType;
        } else if (Long.class.equals(type)) {
            return LongType;
        } else if (Double.class.equals(type)) {
            return DoubleType;
        } else if (Boolean.class.equals(type)) {
            return BooleanType;
        } else {
            throw new UnsupportedOperationException("unsuported type " + type);
        }
    }

    public org.apache.spark.sql.Dataset<Row> getSparkDataset() {
        return sparkDataset;
    }

    @Override
    public List<List<Object>> getDataPoints() {
        List<Row> rows = sparkDataset.collectAsList();
        return rows.stream().map(row -> JavaConverters.seqAsJavaList(row.toSeq()))
                .collect(Collectors.toList());
    }

    @Override
    public List<Component> getDataStructure() {
        if (components == null) {
            StructType schema = sparkDataset.schema();
            components = new ArrayList<>();
            for (StructField field : JavaConverters.asJavaCollection(schema)) {
                components.add(new Component(field.name(), toVtlType(field.dataType()), Role.MEASURE));
            }
        }
        return components;
    }
}