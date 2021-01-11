package fr.insee.vtl.spark;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.*;

/**
 * A wrapper around a spark dataframe
 */
public class SparkDataset implements Dataset {

    private final org.apache.spark.sql.Dataset<Row> sparkDataset;
    private DataStructure dataStructure = null;
    private Map<String, Role> roles = Collections.emptyMap();

    public SparkDataset(org.apache.spark.sql.Dataset<Row> sparkDataset, Map<String, Role> roles) {
        this.sparkDataset = Objects.requireNonNull(sparkDataset);
        this.roles = Objects.requireNonNull(roles);
    }

    public SparkDataset(org.apache.spark.sql.Dataset<Row> sparkDataset) {
        this.sparkDataset = sparkDataset;
    }

    public SparkDataset(Dataset vtlDataset, Map<String, Role> roles, SparkSession spark) {
        List<Row> rows = vtlDataset.getDataPoints().stream().map(points ->
                RowFactory.create(points.toArray(new Object[]{}))
        ).collect(Collectors.toList());

        // TODO: Handle nullable with component
        StructType schema = toSparkSchema(vtlDataset.getDataStructure());

        this.sparkDataset = spark.createDataFrame(rows, schema);
        this.roles = Objects.requireNonNull(roles);
    }

    public SparkDataset(org.apache.spark.sql.Dataset<Row> sparkDataset, DataStructure dataStructure) {
        this.sparkDataset = Objects.requireNonNull(sparkDataset);
        this.dataStructure = Objects.requireNonNull(dataStructure);
    }

    public static StructType toSparkSchema(DataStructure structure) {
        List<StructField> schema = new ArrayList<>();
        for (Component component : structure.values()) {
            schema.add(DataTypes.createStructField(
                    component.getName(),
                    fromVtlType(component.getType()),
                    true
            ));
        }
        return DataTypes.createStructType(schema);
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

    public static DataType fromVtlType(Class<?> type) {
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
    public List<DataPoint> getDataPoints() {
        List<Row> rows = sparkDataset.collectAsList();
        return rows.stream().map(row -> JavaConverters.seqAsJavaList(row.toSeq()))
                .map(row -> new DataPoint(getDataStructure(), row))
                .collect(Collectors.toList());
    }

    @Override
    public Structured.DataStructure getDataStructure() {
        if (dataStructure == null) {
            StructType schema = sparkDataset.schema();
            List<Component> components = new ArrayList<>();
            for (StructField field : JavaConverters.asJavaCollection(schema)) {
                components.add(new Component(
                        field.name(),
                        toVtlType(field.dataType()),
                        roles.getOrDefault(field.name(), Role.MEASURE)
                ));
            }
            dataStructure = new DataStructure(components);
        }
        return dataStructure;
    }
}
