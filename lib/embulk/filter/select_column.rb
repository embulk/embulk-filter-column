Embulk::JavaPlugin.register_filter(
  "select_column", "org.embulk.filter.SelectColumnFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
