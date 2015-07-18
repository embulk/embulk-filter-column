package org.embulk.filter.column;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import com.google.common.base.Optional;

public interface ColumnConfig extends Task
{
    @Config("name")
    public String getName();

    @Config("default")
    @ConfigDefault("null")
    public Optional<Object> getDefault();

    @Config("format")
    @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
    public Optional<String> getFormat();

    @Config("timezone")
    @ConfigDefault("\"UTC\"")
    public Optional<String> getTimezone();
}
