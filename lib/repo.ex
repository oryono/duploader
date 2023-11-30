defmodule Duploader.Repo do
  use Ecto.Repo,
    otp_app: :duploader,
    adapter: Ecto.Adapters.Postgres
end
