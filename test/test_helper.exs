ExUnit.start()

# Exclude :integration tests unless TRACESTAX_INGEST_URL is set.
# When running inside docker-compose.test.yml the env var is always provided,
# so integration tests run automatically in that environment.
unless System.get_env("TRACESTAX_INGEST_URL") do
  ExUnit.configure(exclude: [:integration])
end
