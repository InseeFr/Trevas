package fr.insee.vtl.spark;

import static org.apache.spark.sql.types.DataTypes.*;
import static scala.collection.JavaConverters.mapAsScalaMap;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import scala.Predef;
import scala.collection.JavaConverters;

/** The <code>SparkDataset</code> class is a wrapper around a Spark dataframe. */
public class SparkDataset implements Dataset {

  private final org.apache.spark.sql.Dataset<Row> sparkDataset;
  private DataStructure dataStructure = null;
  private Map<String, Role> roles = Collections.emptyMap();
  private Map<String, String> valuedomains = Collections.emptyMap();

  /**
   * Constructor taking a Spark dataset.
   *
   * @param sparkDataset a Spark dataset.
   */
  public SparkDataset(org.apache.spark.sql.Dataset<Row> sparkDataset) {
    this.sparkDataset = castIfNeeded(sparkDataset);
  }

  /**
   * Constructor taking a Spark dataset and a structure.
   *
   * @param sparkDataset a Spark dataset.
   * @param structure a Data Structure.
   */
  public SparkDataset(org.apache.spark.sql.Dataset<Row> sparkDataset, DataStructure structure) {
    org.apache.spark.sql.Dataset<Row> castedSparkDataset =
        castIfNeeded(Objects.requireNonNull(sparkDataset));
    this.sparkDataset = addMetadata(castedSparkDataset, structure);
    this.roles =
        Objects.requireNonNull(
            structure.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getRole())));
    this.valuedomains =
        Objects.requireNonNull(
            structure.entrySet().stream()
                .filter(e -> null != e.getValue().getValuedomain())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValuedomain())));
  }

  /**
   * Constructor taking a {@link Dataset}, a mapping of component names and roles, and a Spark
   * session.
   *
   * @param vtlDataset a VTL dataset.
   * @param roles a map between component names and their roles in the dataset.
   * @param spark a Spark session to use for the creation of the Spark dataset.
   */
  public SparkDataset(Dataset vtlDataset, Map<String, Role> roles, SparkSession spark) {
    List<Row> rows =
        vtlDataset.getDataPoints().stream()
            .map(points -> RowFactory.create(points.toArray(new Object[] {})))
            .collect(Collectors.toList());
    // TODO: Handle nullable with component
    StructType schema = toSparkSchema(vtlDataset.getDataStructure());
    this.sparkDataset = spark.createDataFrame(rows, schema);
    this.roles = Objects.requireNonNull(roles);
  }

  /**
   * Constructor taking a Spark dataset and a mapping of component names and roles.
   *
   * @param sparkDataset a Spark dataset.
   * @param roles a map between component names and their roles in the dataset.
   */
  public SparkDataset(org.apache.spark.sql.Dataset<Row> sparkDataset, Map<String, Role> roles) {
    org.apache.spark.sql.Dataset<Row> castedSparkDataset =
        castIfNeeded(Objects.requireNonNull(sparkDataset));
    DataStructure dataStructure = fromSparkSchema(sparkDataset.schema(), roles, Map.of());
    this.sparkDataset = addMetadata(castedSparkDataset, dataStructure);
    this.roles = Objects.requireNonNull(roles);
  }

  /** Cast integer and float types to long and double efficiently. */
  private static org.apache.spark.sql.Dataset<Row> castIfNeeded(
      org.apache.spark.sql.Dataset<Row> sparkDataset) {
    StructType schema = sparkDataset.schema();

    // Se construye una lista de expresiones para castear en una sola transformación
    List<Column> castedColumns =
        Arrays.stream(schema.fields())
            .map(
                field -> {
                  DataType type = field.dataType();
                  Column col = sparkDataset.col(field.name());
                  if (type instanceof IntegerType
                      || type instanceof FloatType
                      || type instanceof DecimalType) {
                    return col.cast(
                            type instanceof IntegerType ? DataTypes.LongType : DataTypes.DoubleType)
                        .alias(field.name());
                  }
                  return col;
                })
            .collect(Collectors.toList());

    return sparkDataset.select(castedColumns.toArray(new Column[0]));
  }

  /** Convert Spark schema to VTL DataStructure efficiently. */
  public static DataStructure fromSparkSchema(
      StructType schema, Map<String, Role> roles, Map<String, String> valuedomains) {
    return new DataStructure(
        Arrays.stream(schema.fields())
            .map(
                field ->
                    new Component(
                        field.name(),
                        toVtlType(field.dataType()),
                        handleRole(field, roles),
                        null,
                        handleValuedomain(field, valuedomains)))
            .collect(Collectors.toList()));
  }

  /** Add metadata to dataset in a single transformation step. */
  private static org.apache.spark.sql.Dataset<Row> addMetadata(
      org.apache.spark.sql.Dataset<Row> sparkDataset, DataStructure structure) {
    StructType updatedSchema = toSparkSchema(structure);

    return sparkDataset.select(
        Arrays.stream(updatedSchema.fields())
            .map(field -> sparkDataset.col(field.name()).as(field.name(), field.metadata()))
            .toArray(Column[]::new));
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
      Object vd = null == component.getValuedomain() ? null : component.getValuedomain();
      // TODO: refine nullable strategy
      Map<String, Object> map = new HashMap<>();
      map.put("vtlRole", component.getRole().name());
      map.put("vtlValuedomain", vd);
      scala.collection.immutable.Map<String, Object> md =
          mapAsScalaMap(map).toMap(Predef.$conforms());
      schema.add(
          DataTypes.createStructField(
              component.getName(), fromVtlType(component.getType()), true, new Metadata(md)));
    }
    return DataTypes.createStructType(schema);
  }

  private static Role handleRole(StructField field, Map<String, Role> roles) {
    Role fieldRole;
    if (roles.containsKey(field.name())) {
      fieldRole = roles.get(field.name());
    } else if (field.metadata().contains("vtlRole")) {
      String roleName = field.metadata().getString("vtlRole");
      fieldRole = Role.valueOf(roleName);
    } else {
      fieldRole = Role.MEASURE;
    }
    return fieldRole;
  }

  private static String handleValuedomain(StructField field, Map<String, String> valuedomains) {
    String valuedomain;
    if (valuedomains.containsKey(field.name())) {
      valuedomain = valuedomains.get(field.name());
    } else if (field.metadata().contains("vtlValuedomain")) {
      valuedomain = field.metadata().getString("vtlValuedomain");
    } else {
      valuedomain = null;
    }
    return valuedomain;
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
    return rows.stream()
        .map(row -> JavaConverters.seqAsJavaList(row.toSeq()))
        .map(row -> new DataPoint(getDataStructure(), row))
        .collect(Collectors.toList());
  }

  @Override
  public Structured.DataStructure getDataStructure() {
    if (dataStructure == null) {
      dataStructure = fromSparkSchema(sparkDataset.schema(), roles, valuedomains);
    }
    return dataStructure;
  }
}
