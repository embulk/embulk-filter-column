# Column filter plugin for Embulk

[![Build Status](https://secure.travis-ci.org/sonots/embulk-filter-column.png?branch=master)](http://travis-ci.org/sonots/embulk-filter-column)

A filter plugin for Embulk to filter out columns

## Configuration

- **columns**: columns to retain (array of hash)
  - **name**: name of column (required)
  - **src**: src column name to be copied (optional, default is `name`)
  - **default**: default value used if input is null (optional)
  - **type**: type of the default value (required for `default`)
  - **format**: special option for timestamp column, specify the format of the default timestamp (string, default is `default_timestamp_format`)
  - **timezone**: special option for timestamp column, specify the timezone of the default timestamp (string, default is `default_timezone`)
- **add_columns**: columns to add (array of hash)
  - **name**: name of column (required)
  - **src**: src column name to be copied (either of `src` or `default` is required)
  - **default**: value of column (either of `src` or `default` is required)
  - **type**: type of the default value (required for `default`)
  - **format**: special option for timestamp column, specify the format of the default timestamp (string, default is `default_timestamp_format`)
  - **timezone**: special option for timestamp column, specify the timezone of the default timestamp (string, default is `default_timezone`)
- **drop_columns**: columns to drop (array of hash)
  - **name**: name of column (required)
- **default_timestamp_format**: default timestamp format for timestamp columns (string, default is `%Y-%m-%d %H:%M:%S.%N %z`)
- **default_timezone**: default timezone for timestamp columns (string, default is `UTC`)

## Example (columns)

Say input.csv is as follows:

```
time,id,key,score
2015-07-13,0,Vqjht6YEUBsMPXmoW1iOGFROZF27pBzz0TUkOKeDXEY,1370
2015-07-13,1,VmjbjAA0tOoSEPv_vKAGMtD_0aXZji0abGe7_VXHmUQ,3962
2015-07-13,2,C40P5H1WcBx-aWFDJCI8th6QPEI2DOUgupt_gB8UutE,7323
```

```yaml
filters:
  - type: column
    columns:
      - {name: time, default: "2015-07-13", format: "%Y-%m-%d"}
      - {name: id}
      - {name: key, default: "foo"}
```

reduces columns to only `time`, `id`, and `key` columns as:

```
2015-07-13,0,Vqjht6YEUBsMPXmoW1iOGFROZF27pBzz0TUkOKeDXEY
2015-07-13,1,VmjbjAA0tOoSEPv_vKAGMtD_0aXZji0abGe7_VXHmUQ
2015-07-13,2,C40P5H1WcBx-aWFDJCI8th6QPEI2DOUgupt_gB8UutE
```

Note that column types are automatically retrieved from input data (inputSchema).

## Example (add_columns)

Say input.csv is as follows:

```
time,id,key,score
2015-07-13,0,Vqjht6YEUBsMPXmoW1iOGFROZF27pBzz0TUkOKeDXEY,1370
2015-07-13,1,VmjbjAA0tOoSEPv_vKAGMtD_0aXZji0abGe7_VXHmUQ,3962
2015-07-13,2,C40P5H1WcBx-aWFDJCI8th6QPEI2DOUgupt_gB8UutE,7323
```

```yaml
filters:
  - type: column
    add_columns:
      - {name: d, type: timestamp, default: "2015-07-13", format: "%Y-%m-%d"}
      - {name: copy_id, src: id}
```

add `d` column, and `copy_id` column which is a copy of `id` column as:

```
2015-07-13,0,Vqjht6YEUBsMPXmoW1iOGFROZF27pBzz0TUkOKeDXEY,1370,2015-07-13,0
2015-07-13,1,VmjbjAA0tOoSEPv_vKAGMtD_0aXZji0abGe7_VXHmUQ,3962,2015-07-13,1
2015-07-13,2,C40P5H1WcBx-aWFDJCI8th6QPEI2DOUgupt_gB8UutE,7323,2015-07,13,2
```

## Example (drop_columns)

Say input.csv is as follows:

```
time,id,key,score
2015-07-13,0,Vqjht6YEUBsMPXmoW1iOGFROZF27pBzz0TUkOKeDXEY,1370
2015-07-13,1,VmjbjAA0tOoSEPv_vKAGMtD_0aXZji0abGe7_VXHmUQ,3962
2015-07-13,2,C40P5H1WcBx-aWFDJCI8th6QPEI2DOUgupt_gB8UutE,7323
```

```yaml
filters:
  - type: column
    drop_columns:
      - {name: time}
      - {name: id}
```

drop `time` and `id` columns as:

```
Vqjht6YEUBsMPXmoW1iOGFROZF27pBzz0TUkOKeDXEY,1370
VmjbjAA0tOoSEPv_vKAGMtD_0aXZji0abGe7_VXHmUQ,3962
C40P5H1WcBx-aWFDJCI8th6QPEI2DOUgupt_gB8UutE,7323
```

## JSONPath (like) name

For type: json column, you can specify [JSONPath](http://goessner.net/articles/JsonPath/) for column's name as:

```
$.payload.key1
$.payload.array[0]
$.payload.array[*]
```

EXAMPLE:

* [example/json_columns.yml](example/json_columns.yml)
* [example/json_add_columns.yml](example/json_add_columns.yml)
* [example/json_drop_columns.yml](example/json_drop_columns.yml)

NOTE:

* JSONPath syntax is not fully supported
* Embulk's type: json cannot have timestamp column, so `type: timesatmp` for `add_columns` or `columns` with default is not available
* `src` (to rename or copy columns) for `add_columns` or `columns` is only partially supported yet
  * the json path directory must be same, for example, `{name: $.foo.copy, src: $foo.bar}` works, but `{name: $foo.copy, src: $.bar.baz}` does not work

## ToDo

* Write test

## Development

Run example:

```
$ ./gradlew classpath
$ embulk preview -I lib example/example.yml
```

Run test:

```
$ ./gradlew test
```

Run test with coverage reports:

```
$ ./gradlew test jacocoTestReport
```

open build/reports/jacoco/test/html/index.html

Run checkstyle:

```
$ ./gradlew check
```

Release gem:

```
$ ./gradlew gemPush
```
