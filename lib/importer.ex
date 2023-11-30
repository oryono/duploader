defmodule Duploader.Importer do
  @schemas %{
    "CBA" => Duploader.CreditBorrowerAccounts
  }

  def process_files() do
    directory = "data"

    # datasets = ["BC", "BS", "CAP", "FRA", "CBA", "CCG", "CMC", "IB", "PI", "PIS"]
    datasets = ["CBA"]

    Enum.each(
      datasets,
      fn category ->
        pattern = ~r/^CB\d{3}(\d{8})#{category}\.csv$/i

        files =
          File.ls!(directory)
          |> Enum.filter(&Regex.match?(pattern, &1))

        Enum.map(files, fn file ->
          path = Path.join([directory, file])
          process_file(path, category)
        end)
      end
    )
  end

  defp process_file(path, category) do
    file_stream = File.stream!(path)

    # Read and process the header
    header =
      file_stream
      |> Enum.take(1)
      |> Enum.map(&process_line/1)
      |> Enum.at(0)

    # Process the rest of the file in chunks
    file_stream
    |> Stream.drop(1)
    |> Stream.map(&process_line/1)
    |> Stream.chunk_every(400, 400, :discard)
    |> Task.async_stream(&process_batch(&1, category, header))
    |> Stream.run()
  end

  defp process_line(line) do
    line
    # |> String.trim()
    |> String.split("|", trim: true)
    |> Enum.map(&String.trim(&1, "\""))
    |> Enum.map(&String.trim(&1))
    |> Enum.map(&String.replace(&1, ~r/[^\x01-\x7F]/, ""))
  end

  defp process_batch(batch, category, [_, _, _, submission_date, _, _, _]) do
    data = Enum.map(batch, fn [_first | rest] -> [submission_date | rest] end)

    schema = @schemas[category]

    rows =
      Enum.map(data, fn row ->
        Enum.zip(List.delete_at(schema.__schema__(:fields), 0), row)
      end)

    Enum.each(Enum.chunk_every(rows, 400, 400, :discard), fn batch ->
      schema
      |> Duploader.Repo.insert_all(batch)
    end)
  end
end
