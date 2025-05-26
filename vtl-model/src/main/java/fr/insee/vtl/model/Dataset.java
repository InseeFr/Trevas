package fr.insee.vtl.model;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Structured dataset <code>Dataset</code> is the base interface for structured datasets conforming
 * to the VTL data model.
 *
 * <p>The dataset has a {@link DataStructure} and contains a list of {@link DataPoint}s
 */
public interface Dataset extends Structured {

  /**
   * Returns the data contained in the dataset as a list of data points.
   *
   * <p>Note that the order of the values in the datapoint is in the context of its data structure.
   * It is therefore recommended to access the point by column name or use the getDataAsList method
   * if you plan on using indices.
   *
   * @return The data contained in the dataset as a list of data points.
   */
  List<DataPoint> getDataPoints();

  default List<List<Object>> getDataAsList() {
    var columns = getDataStructure().keySet();
    return getDataPoints().stream()
        .map(dataPoint -> columns.stream().map(dataPoint::get).collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  /**
   * Returns the data contained in the dataset as a list of maps.
   *
   * <p>The returned {@link Map}s are views backed by the {@link DataPointMap} class and do not
   * trigger data copy. Some methods of the Map interface are unsupported.
   */
  default List<Map<String, Object>> getDataAsMap() {
    return getDataPoints().stream().map(DataPointMap::new).collect(Collectors.toList());
  }

  /**
   * The <code>Role</code> <code>Enumeration</code> lists the roles of a component in a dataset
   * structure.
   */
  enum Role {
    /** The component is an identifier in the data structure */
    IDENTIFIER,
    /** The component is a measure in the data structure */
    MEASURE,
    /** The component is an attribute in the data structure */
    ATTRIBUTE,
    /** The component is a viral attribute in the data structure */
    VIRALATTRIBUTE
  }
}
