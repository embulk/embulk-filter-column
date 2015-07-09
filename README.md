# Column filter plugin for Embulk

A filter plugin for Embulk to filter out columns

## Configuration

- **columns**: columns (array of hash, required)
  - **name**: name of column
  - **default**: default value used if input is null

## Example

```yaml
filters:
  - type: column
    columns:
      - {name: id}
      - {name: name, default: 'foo'}
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
