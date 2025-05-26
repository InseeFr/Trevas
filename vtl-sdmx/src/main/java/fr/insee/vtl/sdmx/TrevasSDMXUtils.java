package fr.insee.vtl.sdmx;

import static fr.insee.vtl.model.Structured.Component;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import io.sdmx.api.io.ReadableDataLocation;
import io.sdmx.api.sdmx.constants.TEXT_TYPE;
import io.sdmx.api.sdmx.model.beans.SdmxBeans;
import io.sdmx.api.sdmx.model.beans.base.ComponentBean;
import io.sdmx.api.sdmx.model.beans.base.INamedBean;
import io.sdmx.api.sdmx.model.beans.datastructure.DataStructureBean;
import io.sdmx.api.sdmx.model.beans.transformation.IVtlMappingBean;
import io.sdmx.format.ml.api.engine.StaxStructureReaderEngine;
import io.sdmx.format.ml.engine.structure.reader.v3.StaxStructureReaderEngineV3;
import io.sdmx.utils.core.io.InMemoryReadableDataLocation;
import io.sdmx.utils.core.io.ReadableDataLocationTmp;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TrevasSDMXUtils {

  private static final StaxStructureReaderEngine structureReaderSDMX3 =
      StaxStructureReaderEngineV3.getInstance();

  public static Map<String, Structured.DataStructure> parseDataStructure(SdmxBeans sdmxBeans) {
    Map<String, DataStructureBean> mapping = vtlMapping(sdmxBeans);
    Map<String, DataStructureBean> dataflows = dataflows(sdmxBeans);
    return Stream.concat(
            dataflows.entrySet().stream().filter(e -> !mapping.containsValue(e.getValue())),
            mapping.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, e -> convertStructure(e.getValue())));
  }

  private static Structured.DataStructure convertStructure(DataStructureBean sdmxStructure) {
    List<Component> components =
        sdmxStructure.getComponents().stream()
            .map(
                sdmxComp -> {
                  Dataset.Role role = convertTypeToRole(sdmxComp.getType());
                  String name = sdmxComp.getId();
                  Class<?> type = convertType(sdmxComp);
                  boolean nullable = true;
                  String valuedomain = convertValuedomain(sdmxComp);
                  return new Component(name, type, role, nullable, valuedomain);
                })
            .collect(Collectors.toList());
    return new Structured.DataStructure(components);
  }

  private static Class<?> convertType(ComponentBean sdmxComp) {
    TEXT_TYPE textType = sdmxComp.getTextType();
    if (textType == null) {
      textType = TEXT_TYPE.STRING;
    }
    return switch (textType) {
      case INTEGER, LONG, BIG_INTEGER -> Long.class;
      case FLOAT, DOUBLE, DECIMAL, NUMERIC -> Double.class;
      case BOOLEAN -> Boolean.class;
      // TODO: add the right Duration class (PeriodDuration)
      // case OBSERVATIONAL_TIME_PERIOD:
      // case REPORTING_TIME_PERIOD:
      //    return PeriodDuration.class;
      default -> String.class;
    };
  }

  private static String convertValuedomain(ComponentBean sdmxComp) {
    if (null == sdmxComp.getRepresentation()
        || null == sdmxComp.getRepresentation().getRepresentation()) return null;
    return sdmxComp.getRepresentation().getRepresentation().getMaintainableId();
  }

  private static Dataset.Role convertTypeToRole(ComponentBean.COMPONENT_TYPE type) {
    return switch (type) {
      case MEASURE -> Dataset.Role.MEASURE;
      case ATTRIBUTE, METADATA_ATTRIBUE -> Dataset.Role.ATTRIBUTE;
      case DIMENSION -> Dataset.Role.IDENTIFIER;
      default -> throw new UnsupportedOperationException("unsupported role " + type);
    };
  }

  public static Structured.DataStructure buildStructureFromSDMX3(
      SdmxBeans beans, String structureID) {
    var structures = parseDataStructure(beans);
    return structures.get(structureID);
  }

  public static <T> Collector<T, ?, T> toSingleton() {
    return Collectors.collectingAndThen(
        Collectors.toList(),
        list -> {
          if (list.size() != 1) {
            throw new IllegalStateException();
          }
          return list.get(0);
        });
  }

  public static Map<String, DataStructureBean> dataflows(SdmxBeans sdmxBeans) {
    return sdmxBeans.getDataflows().stream()
        .map(df -> sdmxBeans.getDataStructures(df.getDataStructureRef()))
        .distinct()
        .flatMap(Collection::stream)
        .collect(Collectors.toMap(INamedBean::getId, dataStructureBean -> dataStructureBean));
  }

  public static Map<String, DataStructureBean> vtlMapping(SdmxBeans sdmxBeans) {
    // Find the structure using the mapping.
    // mapping -> dataflow -> structure.
    List<IVtlMappingBean> mappings =
        sdmxBeans.getVtlMappingSchemeBean().stream()
            .flatMap(m -> m.getItems().stream())
            .collect(Collectors.toList());
    return mappings.stream()
        .collect(
            Collectors.toMap(
                IVtlMappingBean::getAlias,
                m ->
                    sdmxBeans.getDataflows(m.getMapped()).stream()
                        .flatMap(
                            flow ->
                                sdmxBeans.getDataStructures(flow.getDataStructureRef()).stream())
                        .collect(toSingleton())));
  }

  public static Structured.DataStructure buildStructureFromSDMX3(
      ReadableDataLocation rdl, String structureID) {
    SdmxBeans sdmxBeans = structureReaderSDMX3.getSdmxBeans(rdl);
    return buildStructureFromSDMX3(sdmxBeans, structureID);
  }

  public static Structured.DataStructure buildStructureFromSDMX3(
      String sdmxDSDPath, String structureID) {
    ReadableDataLocation rdl = new ReadableDataLocationTmp(sdmxDSDPath);
    return buildStructureFromSDMX3(rdl, structureID);
  }

  public static Structured.DataStructure buildStructureFromSDMX3(
      InputStream sdmxDSD, String structureID) {
    ReadableDataLocation rdl = new InMemoryReadableDataLocation(sdmxDSD);
    return buildStructureFromSDMX3(rdl, structureID);
  }
}
