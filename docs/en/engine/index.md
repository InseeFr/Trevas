# VTL Engine

## Maven dependency

```xml=
<dependency>
    <groupId>fr.insee.trevas</groupId>
    <artifactId>vtl-engine</artifactId>
    <version>0.3.0</version>
</dependency>
```

## Instanciation of the execution engine

The execution engine conforms to the [`Script Engine`](https://docs.oracle.com/javase/10/scripting/java-scripting-api.htm#JSJSG109) specification, and can thus be instanciated with the following instuction:

```java=
ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
```

## Input variables

Input variables can be declared to the engine by passing them in key/value form as a `Bindings` object. Trevas provides a simple implementation base on a Java `Map`:

```java=
Map<String, Object> bindingsMap = new HashMap<>();
bindingsMap.put("a", 1);
Bindings bindings = new SimpleBindings(bindingsMap);
ScriptContext context = engine.getContext();
context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
```

The map containing the bindings must be mutable (the script evaluation adds the output bindings to it). The following initialization would raise an exception (`UnsupportedOperationException`) during execution:

```java=
Bindings bindings = new SimpleBindings(Map.of("a", 1));
```

However, it is possible to benefit from the conciseness of `Map.of()` by writing :

```java=
Bindings bindings = new SimpleBindings(new HashMap<>(Map.of("a", 1)));
```

## Script execution

```java=
// The 'a' variable will be looked up in the bindings passed to the engine
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
