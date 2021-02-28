# Dataset - InMemory

Les datasets `InMemoryDataset` permettent de représenter les tables statistiques dans une application Java.

## Exemple

```java=
ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");

Bindings bindings = new SimpleBindings();

// Par défaut, si le rôle d'une variable n'est pas défini, le rôle `MEASURE` lui sera affecté.
InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("var1", "x", "var2", "y", "var3", 5),
                        Map.of("var1", "xx", "var2", "yy", "var3", 10)
                ),
                Map.of("var1", String.class, "var2", String.class, "var3", Long.class),
                Map.of()
);

bindings.put("dataset", dataset);

ScriptContext context = engine.getContext();
context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

String script = "res := dataset[filter var3 > 6];";

try {
    engine.eval(script);
} catch (ScriptException e) {
    e.printStackTrace();
}

Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);

InMemoryDataset res = (InMemoryDataset) outputBindings.get("res");
```
