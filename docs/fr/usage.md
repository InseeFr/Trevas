# Usage

## Import dans un projet Maven

Il faut importer le moteur Trevas dans votre projet via le `pom.xml` :

```xml
<dependency>
    <groupId>fr.insee.trevas</groupId>
    <artifactId>vtl-engine</artifactId>
    <version>x.y.z</version>
</dependency>
```

## Utilisation du moteur

Pour instancier le moteur VTL 2.0, il suffit de créer un `ScriptEngine`:

```java
ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
```

Il est possible d'ajouter des 'bindings' comme suit:

```java
ScriptContext context = engine.getContext();
context.setBindings(jsonBindings, ScriptContext.ENGINE_SCOPE);
```

Pour exécuter le script et récupérer les résultats:

```java
try {
    engine.eval(script);
    Bindings outputBindings = context.getBindings(ScriptContext.ENGINE_SCOPE);
    response.send(outputBindings);
} catch (ScriptException e) {
    processErrors(e, request, response);
}
```

Où `processErrors` peut être, par exemple:

```java
private <T> T processErrors(Throwable ex, ServerRequest request, ServerResponse response) {
    LOGGER.log(Level.FINE, "Error", ex);
    if (ex instanceof VtlScriptException) {
        JsonObject jsonErrorObject = JSON.createObjectBuilder()
                .add("error", ex.getMessage())
                .build();
        response.status(Http.Status.BAD_REQUEST_400).send(jsonErrorObject);
    }

    JsonObject jsonErrorObject = JSON.createObjectBuilder()
            .add("error", ex.getMessage())
            .build();
    response.status(Http.Status.INTERNAL_SERVER_ERROR_500).send(jsonErrorObject);

    return null;
}
```

## Sérialisation des jeux de données

Au lieu de gérer vous-mêmes les instances de la classe `Dataset` définie dans Trevas, vous pouvez utiliser un module existant basé sur Jackson.

### Dépendances

```xml
<dependency>
    <groupId>fr.insee.trevas</groupId>
    <artifactId>vtl-jackson</artifactId>
    <version>x.y.z</version>
    <scope>compile</scope>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.11.2</version>
</dependency>
```

### Initialisation du Jackson Mapper

Grâce à ce module, il est possible d'enregistrer une fois seulement le module Jackson dans votre application:

```java
ObjectMapper mapper = new ObjectMapper();
mapper.registerModule(new TrevasModule());
```

En le fournissant à votre serveur web, `Dataset`, `Component` et `Bindings` seront sérialisés et désérialisés automatiquement.

### Modèle JSON pour les bindings

Les bindings JSON couvrent les scalaires et les jeux de données.

Pour inclure un `Dataset`, il est nécessaire de respecter le modèle suivant :

```json
{
    "dataStructure": [
        {"name": "my_name", "type": "STRING", "role": "IDENTIFIER"},
        {"age": "my_age", "type": "INTEGER", "role": "MEASURE"},
    ],
    "dataPoints": [
        ["name1", 20],
        ["name2", 30],
        ...
    ]
}
```

La valeur de `type` doit être parmi : `STRING`, `INTEGER`, `NUMBER`, `BOOLEAN`.

La valeur de `role` doit être parmi : `IDENTIFIER`, `MEASURE`, `ATTRIBUTE`.
