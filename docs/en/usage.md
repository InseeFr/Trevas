# Usage

## Prerequisite

There are WIP to deploy Trevas into Maven central repository.

While waiting for this step, you can use it locally:

```bash
git clone https://github.com/InseeFr/Trevas.git
cd Trevas
mvn package
```

You have to add it finally to your classpath.

## Importing into Maven project

Trevas engine has to be added to your project into your `pom.xml`:

```xml
<dependency>
    <groupId>fr.insee.trevas</groupId>
    <artifactId>vtl-engine</artifactId>
    <version>x.y.z</version>
</dependency>
```

## Engine usage

To instantiate VTL 2.0 engine, we simply have to create a `ScriptEngine`:

```java
ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
```

If we want to add input bindings, do it as follow:

```java
ScriptContext context = engine.getContext();
context.setBindings(jsonBindings, ScriptContext.ENGINE_SCOPE);
```

To execute the script and get results, just do:

```java
try {
    engine.eval(script);
    Bindings outputBindings = context.getBindings(ScriptContext.ENGINE_SCOPE);
    response.send(outputBindings);
} catch (ScriptException e) {
    processErrors(e, request, response);
}
```

Where `processErrors` can be for instance:

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

## Dataset serialization

Instead of dealing yourself with Dataset objects, defined into Trevas Model module, a dedicated module based on Jackson is available into Trevas.

### Dependencies

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

### Initialization of Jackson Mapper

Thanks to the module, it's possible to register once the Trevas Jackson module in your application:

```java
ObjectMapper mapper = new ObjectMapper();
mapper.registerModule(new TrevasModule());
```

Giving it to your webserver, `Dataset`, `Component`, `Bindings` will be serialized/deserialized automatically.

### Bindings JSON model

Scalar objects & datasets are enabled into JSON bindings.

To include a Dataset, you have to respect the following model:

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

Types have to be one of: `STRING`, `INTEGER`, `NUMBER`, `BOOLEAN`.

Roles have to be one of: `IDENTIFIER`, `MEASURE`, `ATTRIBUTE`.
