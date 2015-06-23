# Column filter plugin for Embulk

A filter plugin for Embulk to filter out columns

## Configuration

- **columns**: column names and types (array of hash, required)

## Example

```yaml
filters:
  - type: column
    coolumns
    - {name: id, type: long}
    - {name: name, type: string}
```


## Build

```
$ ./gradlew gem
```
