Embulk::JavaPlugin.register_filter(
  "column", "org.embulk.filter.ColumnFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
