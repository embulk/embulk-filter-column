# Column filter plugin for Embulk

A filter plugin for Embulk to filter out columns

## Configuration

- **columns**: columns (array of hash, required)
  - **name**: name of column
  - **default**: default value used if input is null
  - **format**: special option for timestamp column, specify the format of timestamp default (string, default is `%Y-%m-%d %H:%M:%S.%N %z`)
  - **timezone**: special option for timestamp column, specify the timezone of timestamp default (string, default is `UTC`)

NOTE: column type is automatically retrieved from input data (inputSchema)

## Example

```yaml
filters:
  - type: column
    columns:
      - {name: time, default: "2015-07-13", format: "%Y-%m-%d"}
      - {name: id}
      - {name: name, default: "foo"}
```

reduces columns to only `time`, `id`, and `name` columns.

## Development

Run example:

```
$ ./gradlew classpath
$ embulk run -I lib example.yml
```

Run test:

```
$ ./gradew test
```

Release gem:

```
$ ./gradlew gemPush
```
