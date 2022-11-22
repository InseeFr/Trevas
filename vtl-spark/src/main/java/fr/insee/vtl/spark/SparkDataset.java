package fr.insee.vtl.spark;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Predef;
import scala.collection.JavaConverters;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static scala.collection.JavaConverters.mapAsScalaMap;

/**
 * The <code>SparkDataset</code> class is a wrapper around a Spark dataframe.
 */
public class SparkDataset implements Dataset {

    private final org.apache.spark.sql.Dataset<Row> sparkDataset;
    private DataStructure dataStructure = null;
    private Map<String, Role> roles = Collections.emptyMap();

    /**
     * Constructor taking a Spark dataset and a mapping of component names and roles.
     *
     * @param sparkDataset a Spark dataset.
     * @param roles        a map between component names and their roles in the dataset.
     */
    public SparkDataset(org.apache.spark.sql.Dataset<Row> sparkDataset, Map<String, Role> roles) {
        var castedSparkDataset = castIfNeeded(Objects.requireNonNull(sparkDataset));
        var dataStructure = fromSparkSchema(sparkDataset.schema(), roles);
        this.sparkDataset = addMetadata(castedSparkDataset, dataStructure);
        this.roles = Objects.requireNonNull(roles);
    }

    // loadS3('s3a://') -> vtl.Dataset()
    // readSpark('s3a://foo/bar') -> spark.read()... -> new SparkDataset(ds)


    /**
     * Constructor taking a Spark dataset.
     *
     * @param sparkDataset a Spark dataset.
     */
    public SparkDataset(org.apache.spark.sql.Dataset<Row> sparkDataset) {
        this.sparkDataset = castIfNeeded(sparkDataset);
    }

    /**
     * Constructor taking a {@link Dataset}, a mapping of component names and roles, and a Spark session.
     *
     * @param vtlDataset a VTL dataset.
     * @param roles      a map between component names and their roles in the dataset.
     * @param spark      a Spark session to use for the creation of the Spark dataset.
     */
    public SparkDataset(Dataset vtlDataset, Map<String, Role> roles, SparkSession spark) {
        List<Row> rows = vtlDataset.getDataPoints().stream().map(points -> RowFactory.create(points.toArray(new Object[]{}))).collect(Collectors.toList());

        // TODO: Handle nullable with component
        StructType schema = toSparkSchema(vtlDataset.getDataStructure());

        this.sparkDataset = spark.createDataFrame(rows, schema);
        this.roles = Objects.requireNonNull(roles);
    }

    /**
     * Cast integer and float types to long and double.
     */
    private static org.apache.spark.sql.Dataset<Row> castIfNeeded(org.apache.spark.sql.Dataset<Row> sparkDataset) {
        var casted = sparkDataset;
        StructType schema = sparkDataset.schema();
        for (StructField field : JavaConverters.asJavaCollection(schema)) {
            if (IntegerType.sameType(field.dataType())) {
                casted = casted.withColumn(field.name(),
                        casted.col(field.name()).cast(LongType));
            } else if (FloatType.sameType(field.dataType())) {
                casted = casted.withColumn(field.name(),
                        casted.col(field.name()).cast(DoubleType));
            } else if (DecimalType.class.equals(field.dataType().getClass())) {
                casted = casted.withColumn(field.name(),
                        casted.col(field.name()).cast(DoubleType));
            }
        }
        return casted;
    }

    private static org.apache.spark.sql.Dataset<Row> addMetadata(org.apache.spark.sql.Dataset<Row> sparkDataset, DataStructure structure) {
        var casted = sparkDataset;
        for (StructField field : JavaConverters.asJavaCollection(toSparkSchema(structure))) {
            String name = field.name();
            casted = casted.withColumn(name, casted.col(name), field.metadata());
        }
        return casted;
    }

    /**
     * Transforms a {@link DataStructure} into a Spark schema.
     *
     * @param structure the dataset structure to transform
     * @return The resulting Spark schema (<code>StructType</code> object).
     */
    public static StructType toSparkSchema(DataStructure structure) {
        List<StructField> schema = new ArrayList<>();
        for (Component component : structure.values()) {
            // TODO: refine nullable strategy
            var md = mapAsScalaMap(Map.of("vtlRole", (Object) component.getRole().name()))
                    .toMap(Predef.$conforms());
            schema.add(DataTypes.createStructField(
                    component.getName(),
                    fromVtlType(component.getType()),
                    true,
                    new Metadata(md)
            ));
        }
        return DataTypes.createStructType(schema);
    }

    public static DataStructure fromSparkSchema(StructType schema, Map<String, Role> roles) {
        List<Component> components = new ArrayList<>();
        for (StructField field : JavaConverters.asJavaCollection(schema)) {

            Role fieldRole = roles.getOrDefault(field.name(), Role.MEASURE);
            if (field.metadata().contains("vtlRole")) {
                var roleName = field.metadata().getString("vtlRole");
                fieldRole = Role.valueOf(roleName);
            }

            components.add(new Component(
                    field.name(),
                    toVtlType(field.dataType()),
                    fieldRole,
                    null
            ));
        }
        return new DataStructure(components);
    }

    /**
     * Translates a Spark data type into a VTL data type.
     *
     * @param dataType the Spark {@link DataType} to translate.
     * @return The corresponding VTL data type as a class.
     */
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
        } else if (DecimalType.class.equals(dataType.getClass())) {
            return Double.class;
        } else if (DateType.sameType(dataType)) {
            return Instant.class;
        } else if (TimestampType.sameType(dataType)) {
            return Instant.class;
        } else {
            throw new UnsupportedOperationException("unsupported type " + dataType);
        }
    }

    /**
     * Translates a VTL data type into a Spark data type.
     *
     * @param type the VTL data type to translate (as a class).
     * @return The corresponding Spark {@link DataType}.
     */
    public static DataType fromVtlType(Class<?> type) {
        if (String.class.equals(type)) {
            return StringType;
        } else if (Long.class.equals(type)) {
            return LongType;
        } else if (Double.class.equals(type)) {
            return DoubleType;
        } else if (Boolean.class.equals(type)) {
            return BooleanType;
        } else if (Instant.class.equals(type)) {
            return TimestampType;
        } else if (LocalDate.class.equals(type)) {
            return DateType;
        } else {
            throw new UnsupportedOperationException("unsupported type " + type);
        }
    }

    /**
     * Returns the Spark dataset.
     *
     * @return The Spark dataset.
     */
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
            dataStructure = fromSparkSchema(sparkDataset.schema(), roles);
        }
        return dataStructure;
    }
}
