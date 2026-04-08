defmodule Oban.Migrations.Postgres.V14 do
  @moduledoc false

  use Ecto.Migration

  def up(%{quoted_prefix: quoted}) do
    execute """
    ALTER TYPE #{quoted}.oban_job_state ADD VALUE IF NOT EXISTS 'suspended' BEFORE 'scheduled'
    """
  end

  def down(%{quoted_prefix: quoted}) do
    execute "UPDATE #{quoted}.oban_jobs SET state = 'scheduled' WHERE state = 'suspended'"
  end
end
