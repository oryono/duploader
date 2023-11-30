import Config

config :duploader, Duploader.Repo,
  adapter: Ecto.Adapters.Postgres,
  username: "postgres",
  password: "postgres",
  database: "duploder",
  hostname: "localhost",
  pool_size: 20,
  log: false
