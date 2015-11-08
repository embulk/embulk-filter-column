# Column filter plugin for Embulk

[![Build Status](https://secure.travis-ci.org/sonots/embulk-filter-column.png?branch=master)](http://travis-ci.org/sonots/embulk-filter-column)

A filter plugin for Embulk to filter out columns

## Configuration

- **columns**: columns to retain (array of hash)
  - **name**: name of column (required)
  - **type**: type of column (required to add)
  - **default**: default value used if input is null (required to add)
  - **format**: special option for timestamp column, specify the format of the default timestamp (string, default is `default_timestamp_format`)
  - **timezone**: special option for timestamp column, specify the timezone of the default timestamp (string, default is `default_timezone`)
  - **src**: src value used if input is null (optional)
- **add_columns**: columns to add (array of hash)
  - **name**: name of column (required)
  - **type**: type of column (required)
  - **default**: value of column (required)
  - **format**: special option for timestamp column, specify the format of the default timestamp (string, default is `default_timestamp_format`)
  - **timezone**: special option for timestamp column, specify the timezone of the default timestamp (string, default is `default_timezone`)
  - **src**: src value of column (optional)
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
      - {key: time, default: "2015-07-13", format: "%Y-%m-%d"}
      - {key: id}
      - {key: key, default: "foo"}
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
      - {key: d, type: timestamp, default: "2015-07-13", format: "%Y-%m-%d"}
```

add `d` column as:

```
2015-07-13,0,Vqjht6YEUBsMPXmoW1iOGFROZF27pBzz0TUkOKeDXEY,1370,2015-07-13
2015-07-13,1,VmjbjAA0tOoSEPv_vKAGMtD_0aXZji0abGe7_VXHmUQ,3962,2015-07-13
2015-07-13,2,C40P5H1WcBx-aWFDJCI8th6QPEI2DOUgupt_gB8UutE,7323,2015-07,13
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
      - {key: time}
      - {key: id}
```

drop `time` and `id` columns as:

```
Vqjht6YEUBsMPXmoW1iOGFROZF27pBzz0TUkOKeDXEY,1370
VmjbjAA0tOoSEPv_vKAGMtD_0aXZji0abGe7_VXHmUQ,3962
C40P5H1WcBx-aWFDJCI8th6QPEI2DOUgupt_gB8UutE,7323
```

## ToDo

* Write test

## Development

Run example:

```
$ ./gradlew classpath
$ embulk run -I lib example.yml
```

Run test:

```
$ ./gradlew test
```

Release gem:

```
$ ./gradlew gemPush
```
