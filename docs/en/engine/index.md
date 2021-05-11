# VTL Engine

## Dependency

```xml=
<dependency>
    <groupId>fr.insee.trevas</groupId>
    <artifactId>vtl-engine</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Instanciation of the execution engine

The execution engine conforms to the [`Script Engine`](https://docs.oracle.com/javase/10/scripting/java-scripting-api.htm#JSJSG109) specification, and can thus be instanciated with the following instuction:

```java=
ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
```

## Input variables

Input variables can be declared to the engine by passing them in key/value form in a `Bindings` object.

```java=
Bindings bindings = new SimpleBindings(Map.of("a", 1));
ScriptContext context = engine.getContext();
context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
```

## Script execution

```java=
// The 'a' will be looked up in the engine bindings
String script = "res := a + 1;";
try {
    engine.eval(script);
} catch (ScriptException e) {
    e.printStackTrace();
}
```

## Retrieving the results

All the input and output variables are accessible via the engine context:

```java=
Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
```

It is then possible to access a particular variable:

```java=
outputBindings.get("res");
```

## Dataset

The data sets are represented as instances of `Dataset`.

These `Dataset` are composed of a `dataStructure` and a `dataPoints` members.

The `dataStructure` are composed of `Component` members, each having a `name`, a `role` and a `type`.

The values in the `dataPoints` of components that have a `IDENTIFIER` role must be unique.

Two subclasses of `Dataset` are exposed:

- `InMemoryDataset`: allows the creation of a `Dataset` within the JVM ([see details](./in-memory.md))
- `SparkDataset`: allows the creation of a `Dataset` to be transferred to Spark ([see details](./spark.md))
