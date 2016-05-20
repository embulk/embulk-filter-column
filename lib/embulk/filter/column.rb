Embulk::JavaPlugin.register_filter(
  "column", "org.embulk.filter.column.ColumnFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
