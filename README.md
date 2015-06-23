# Column filter plugin for Embulk

A filter plugin for Embulk to filter out columns

## Configuration

- **columns**: column names (array of hash, required)

## Example

```yaml
filters:
  - type: column
    columns
    - {name: id}
    - {name: name}
```


## Build

```
$ ./gradlew gem
```
