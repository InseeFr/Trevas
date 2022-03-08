# VTL Engine

## Dépendance Maven

```xml=
<dependency>
    <groupId>fr.insee.trevas</groupId>
    <artifactId>vtl-engine</artifactId>
    <version>0.3.0</version>
</dependency>
```

## Instanciation du moteur d'exécution

Le moteur d'exécution répondant à la spécification [`Script Engine`](https://docs.oracle.com/javase/10/scripting/java-scripting-api.htm#JSJSG109), il est instanciable via l'instruction suivante :

```java=
ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
```

## Variable entrantes

Des variables peuvent être déclarées en entrée du moteur, en les affectant sous forme de clés / valeurs au sein d'un objet `Bindings`. Trevas fournit une implémentation simple basée sur une `Map`Java :

```java=
Map<String, Object> bindingsMap = new HashMap<>();
bindingsMap.put("a", 1);
Bindings bindings = new SimpleBindings(bindingsMap);
ScriptContext context = engine.getContext();
context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
```

## Exécution d'un script

```java=
// La variable 'a' sera recherchée dans les bindings passés au moteur
String script = "res := a + 1;";
try {
    engine.eval(script);
} catch (ScriptException e) {
    e.printStackTrace();
}
```

## Récupération des résultats

Toutes les variables entrantes ou créées sont accessibles via le contexte du moteur :

```java=
Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
```

Il est alors possible d'accéder à l'une d'elles en particulier :

```java=
outputBindings.get("res");
```

## Dataset

Les tables de données sont représentées sous forme de `Dataset`.

Les `Dataset` sont composés d'une `dataStructure` et de `dataPoints`.

Les `dataStructure` sont composées de `Component`, ayant un `name`, un `role` et un `type`.

Les valeurs des variables ayant pour rôle `IDENTIFIER` des `dataPoints` doivent être uniques.

Deux instances de `Dataset` sont exposées :

- `InMemoryDataset` : permet de définir un Dataset au sein de la JVM ([voir en détail](./in-memory.md))
- `SparkDataset` : permet de définir un Dataset à transférer à Spark ([voir en détail](./spark.md))
