# VTL Jackson

## Dépendances

```xml=
<dependency>
    <groupId>fr.insee.trevas</groupId>
    <artifactId>vtl-jackson</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.12.1</version>
</dependency>
```

## Objets supportés

- Dataset
- Component
- Bindings

## Json / Dataset

La représentation Json d'un Dataset est définie ainsi :

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

## Déclaration du module

Le module peut être déclaré globalement à l'échelle du projet client :

```java=
public ObjectMapper objectMapper() {
    return new ObjectMapper()
            .registerModule(new TrevasModule());
}
```

## Exemple

### Désérialisation

```java=
ObjectMapper objectMapper = new ObjectMapper();
objectMapper.readValue(json, Dataset.class);
```
