# VTL Jackson

## Dependencies

```xml=
<dependency>
    <groupId>fr.insee.trevas</groupId>
    <artifactId>vtl-jackson</artifactId>
    <version>0.1.0</version>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.12.1</version>
</dependency>
```

## Supported objects

- Dataset
- Component
- Bindings

## JSON dataset

The JSON representation of a `Dataset` is defined as:

```json
{
  "dataStructure": [
    { "name": "id", "type": "STRING", "role": "IDENTIFIER" },
    { "name": "x", "type": "INTEGER", "role": "MEASURE" },
    { "name": "y", "type": "FLOAT", "role": "MEASURE" }
  ],
  "dataPoints": [
    ["0001", 10, 50.5],
    ["0002", 20, -8],
    ["0003", 1000, 0],
    ["0004", 1, 4.5]
  ]
}
```

## Declaration of a module

A module can be globally declared for the client project:

```java=
public ObjectMapper objectMapper() {
    return new ObjectMapper()
            .registerModule(new TrevasModule());
}
```

## Example

### Deserialization

```java=
ObjectMapper objectMapper = new ObjectMapper();
objectMapper.readValue(json, Dataset.class);
```
