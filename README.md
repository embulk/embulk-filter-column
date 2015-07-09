# Column filter plugin for Embulk

A filter plugin for Embulk to filter out columns

## Configuration

- **columns**: column names (array of hash, required)

## Example

```yaml
filters:
  - type: column
    columns:
      - {name: id}
      - {name: name}
```

reduces columns to only `id` and `name` columns.

## Development

Run example:

```
$ ./gradlew classpath
$ embulk run -I lib example.yml
```

Release gem:

```
$ ./gradlew gemPush
```
