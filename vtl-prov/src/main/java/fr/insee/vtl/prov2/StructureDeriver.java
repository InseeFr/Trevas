package fr.insee.vtl.prov2;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured.Component;
import fr.insee.vtl.model.Structured.DataStructure;
import fr.insee.vtl.prov2.PendingOp.Aggr;
import fr.insee.vtl.prov2.PendingOp.Apply;
import fr.insee.vtl.prov2.PendingOp.Arithmetic;
import fr.insee.vtl.prov2.PendingOp.Calc;
import fr.insee.vtl.prov2.PendingOp.Check;
import fr.insee.vtl.prov2.PendingOp.CheckDatapoint;
import fr.insee.vtl.prov2.PendingOp.Drop;
import fr.insee.vtl.prov2.PendingOp.ExistsIn;
import fr.insee.vtl.prov2.PendingOp.Filter;
import fr.insee.vtl.prov2.PendingOp.Identity;
import fr.insee.vtl.prov2.PendingOp.Join;
import fr.insee.vtl.prov2.PendingOp.Keep;
import fr.insee.vtl.prov2.PendingOp.Membership;
import fr.insee.vtl.prov2.PendingOp.PassThrough;
import fr.insee.vtl.prov2.PendingOp.Pivot;
import fr.insee.vtl.prov2.PendingOp.Rename;
import fr.insee.vtl.prov2.PendingOp.SetOp;
import fr.insee.vtl.prov2.PendingOp.Sub;
import fr.insee.vtl.prov2.PendingOp.Unpivot;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Derives a {@link DataStructure} from a {@link PendingOp} when the structure oracle did not bind
 * the LHS (or for anonymous clause intermediates, which are never engine-bound).
 */
final class StructureDeriver {

  private final Function<String, DataStructure> structures;

  StructureDeriver(Function<String, DataStructure> structures) {
    this.structures = structures;
  }

  DataStructure derive(PendingOp op) {
    if (op instanceof Identity id) {
      return new DataStructure(require(id.datasetId()));
    }
    if (op instanceof Arithmetic arithmetic) {
      return new DataStructure(require(arithmetic.operandIds().get(0)));
    }
    if (op instanceof Calc calc) {
      return deriveCalc(require(calc.srcId()), calc.types());
    }
    if (op instanceof Aggr aggr) {
      return deriveAggr(require(aggr.srcId()), aggr.types(), aggr.groupBy());
    }
    if (op instanceof Filter filter) {
      return new DataStructure(require(filter.srcId()));
    }
    if (op instanceof Sub sub) {
      return new DataStructure(require(sub.srcId()));
    }
    if (op instanceof Keep keep) {
      return deriveKeep(require(keep.srcId()), keep.columns());
    }
    if (op instanceof Drop drop) {
      return deriveDrop(require(drop.srcId()), drop.columns());
    }
    if (op instanceof Rename rename) {
      return deriveRename(require(rename.srcId()), rename.renameFrom());
    }
    if (op instanceof Join join) {
      return deriveJoin(join.operandIds());
    }
    if (op instanceof SetOp setOp) {
      return new DataStructure(require(setOp.operandIds().get(0)));
    }
    if (op instanceof CheckDatapoint check) {
      return deriveCheckDatapoint(require(check.srcId()));
    }
    if (op instanceof Check check) {
      return deriveCheck(
          require(check.srcId()),
          check.imbalanceId() == null ? null : require(check.imbalanceId()));
    }
    if (op instanceof PassThrough pass) {
      return new DataStructure(require(pass.srcId()));
    }
    if (op instanceof ExistsIn existsIn) {
      return deriveExistsIn(require(existsIn.leftId()));
    }
    if (op instanceof Pivot pivot) {
      return derivePivot(
          require(pivot.srcId()),
          pivot.idComponent(),
          pivot.measureComponent(),
          pivot.pivotedColumns());
    }
    if (op instanceof Unpivot unpivot) {
      return deriveUnpivot(
          require(unpivot.srcId()), unpivot.idComponent(), unpivot.measureComponent());
    }
    if (op instanceof Membership membership) {
      return deriveMembership(require(membership.srcId()), membership.component());
    }
    if (op instanceof Apply apply) {
      return deriveApply(require(apply.srcId()), apply.measureName(), apply.measureType());
    }
    throw new IllegalStateException("unhandled pending op " + op.getClass().getName());
  }

  private DataStructure require(String datasetId) {
    DataStructure structure = structures.apply(datasetId);
    if (structure == null) {
      throw new IllegalStateException("unknown structure for " + datasetId);
    }
    return structure;
  }

  private DataStructure deriveJoin(List<String> operandIds) {
    List<Component> components = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();
    for (String operandId : operandIds) {
      for (Component component : require(operandId).componentsInOrder()) {
        if (seen.add(component.getName())) {
          components.add(new Component(component));
        }
      }
    }
    return new DataStructure(components);
  }

  private static DataStructure deriveApply(
      DataStructure src, String measureName, Class<?> measureType) {
    List<Component> components = copyIdentifiers(src);
    components.add(new Component(measureName, measureType, Dataset.Role.MEASURE));
    return new DataStructure(components);
  }

  private static DataStructure deriveMembership(DataStructure src, String componentName) {
    Component selected = src.get(componentName);
    if (selected == null) {
      throw new IllegalStateException("unknown membership component " + componentName);
    }
    List<Component> components = copyIdentifiers(src);
    components.add(new Component(selected));
    return new DataStructure(components);
  }

  private static List<Component> copyIdentifiers(DataStructure src) {
    List<Component> components = new ArrayList<>();
    for (Component component : src.componentsInOrder()) {
      if (component.isIdentifier()) {
        components.add(new Component(component));
      }
    }
    return components;
  }

  private static DataStructure deriveUnpivot(
      DataStructure src, String idComponent, String measureComponent) {
    Class<?> measureType = null;
    List<Component> components = new ArrayList<>();
    for (Component component : src.componentsInOrder()) {
      if (component.isIdentifier()) {
        components.add(new Component(component));
      } else if (component.isMeasure()) {
        if (measureType == null) {
          measureType = component.getType();
        }
      }
    }
    if (measureType == null) {
      measureType = Long.class;
    }
    components.add(new Component(idComponent, String.class, Dataset.Role.IDENTIFIER));
    components.add(new Component(measureComponent, measureType, Dataset.Role.MEASURE));
    return new DataStructure(components);
  }

  private static DataStructure derivePivot(
      DataStructure src, String idComponent, String measureComponent, List<String> pivotedColumns) {
    Component measure = src.get(measureComponent);
    if (measure == null) {
      throw new IllegalStateException("unknown pivot measure " + measureComponent);
    }
    List<Component> components = new ArrayList<>();
    for (Component component : src.componentsInOrder()) {
      String name = component.getName();
      if (name.equals(idComponent) || name.equals(measureComponent)) {
        continue;
      }
      components.add(new Component(component));
    }
    for (String pivoted : pivotedColumns) {
      components.add(new Component(pivoted, measure.getType(), Dataset.Role.MEASURE));
    }
    return new DataStructure(components);
  }

  /** Fallback when the engine did not bind the LHS — mirrors Trevas {@code all} output. */
  private static DataStructure deriveCheckDatapoint(DataStructure src) {
    List<Component> components = new ArrayList<>(src.componentsInOrder());
    components.add(new Component("ruleid", String.class, Dataset.Role.IDENTIFIER));
    components.add(new Component("bool_var", Boolean.class, Dataset.Role.MEASURE));
    components.add(new Component("errorcode", String.class, Dataset.Role.MEASURE));
    components.add(new Component("errorlevel", Long.class, Dataset.Role.MEASURE));
    return new DataStructure(components);
  }

  /** Left identifiers + boolean measure (engine unsupported → pure derive). */
  private static DataStructure deriveExistsIn(DataStructure left) {
    List<Component> components = new ArrayList<>(left.getIdentifiers());
    components.add(new Component("bool_var", Boolean.class, Dataset.Role.MEASURE));
    return new DataStructure(components);
  }

  /**
   * Trevas simple {@code check}: keep operand structure, rename imbalance measure to {@code
   * imbalance} when present, append {@code errorcode}/{@code errorlevel} (String when no literals).
   */
  private static DataStructure deriveCheck(DataStructure src, DataStructure imbalance) {
    List<Component> components = new ArrayList<>(src.componentsInOrder());
    if (imbalance != null) {
      Class<?> imbType = Long.class;
      for (Component component : imbalance.values()) {
        if (component.isMeasure()) {
          imbType = component.getType();
          break;
        }
      }
      components.add(new Component("imbalance", imbType, Dataset.Role.MEASURE));
    }
    components.add(new Component("errorcode", String.class, Dataset.Role.MEASURE));
    components.add(new Component("errorlevel", String.class, Dataset.Role.MEASURE));
    return new DataStructure(components);
  }

  private static DataStructure deriveAggr(
      DataStructure src, Map<String, Class<?>> aggrTypes, List<String> groupBy) {
    List<Component> components = new ArrayList<>();
    for (String key : groupBy) {
      Component component = src.get(key);
      if (component == null) {
        throw new IllegalStateException("unknown group-by component " + key);
      }
      components.add(
          new Component(component.getName(), component.getType(), Dataset.Role.IDENTIFIER));
    }
    for (Map.Entry<String, Class<?>> entry : aggrTypes.entrySet()) {
      components.add(new Component(entry.getKey(), entry.getValue(), Dataset.Role.MEASURE));
    }
    return new DataStructure(components);
  }

  private static DataStructure deriveCalc(DataStructure src, Map<String, Class<?>> calcTypes) {
    List<Component> components = new ArrayList<>(src.componentsInOrder());
    for (Map.Entry<String, Class<?>> entry : calcTypes.entrySet()) {
      String name = entry.getKey();
      Class<?> type = entry.getValue();
      int existing = -1;
      for (int i = 0; i < components.size(); i++) {
        if (components.get(i).getName().equals(name)) {
          existing = i;
          break;
        }
      }
      Component component = new Component(name, type, Dataset.Role.MEASURE);
      if (existing >= 0) {
        components.set(existing, component);
      } else {
        components.add(component);
      }
    }
    return new DataStructure(components);
  }

  private static DataStructure deriveKeep(DataStructure src, List<String> columns) {
    List<Component> kept = new ArrayList<>();
    for (Component component : src.componentsInOrder()) {
      if (component.isIdentifier() || columns.contains(component.getName())) {
        kept.add(component);
      }
    }
    return new DataStructure(kept);
  }

  private static DataStructure deriveDrop(DataStructure src, List<String> columns) {
    Set<String> dropped = new LinkedHashSet<>(columns);
    List<Component> kept = new ArrayList<>();
    for (Component component : src.componentsInOrder()) {
      if (component.isIdentifier() || !dropped.contains(component.getName())) {
        kept.add(component);
      }
    }
    return new DataStructure(kept);
  }

  private static DataStructure deriveRename(DataStructure src, Map<String, String> renameFrom) {
    Map<String, String> fromTo = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : renameFrom.entrySet()) {
      fromTo.put(entry.getValue(), entry.getKey());
    }
    List<Component> renamed = new ArrayList<>();
    for (Component component : src.componentsInOrder()) {
      String name = fromTo.getOrDefault(component.getName(), component.getName());
      renamed.add(new Component(name, component.getType(), component.getRole()));
    }
    return new DataStructure(renamed);
  }
}
