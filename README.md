# SelectColumn filter plugin for Embulk

A filter plugin for Embulk to select columns

## Configuration

- **columns**: column names (array, required)

## Example

```yaml
filters:
  - type: select_column
    columns: [id, name]
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
