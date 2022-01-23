defmodule RefElixir.FlowPipeline do
  @moduledoc """
  Documentation for `RefElixir.FlowPipeline`.
  """

  require Logger
  require Flow
  require Benchee
  alias NimbleCSV.RFC4180, as: CSV

  require Application
  @experian_endpoint Application.compile_env(:ref_elixir_flow_pipeline, :experian_endpoint)
  @equifax_endpoint Application.compile_env(:ref_elixir_flow_pipeline, :equifax_endpoint)
  @aml_check_endpoint Application.compile_env(:ref_elixir_flow_pipeline, :aml_check_endpoint)
  @fraud_check_endpoint Application.compile_env(:ref_elixir_flow_pipeline, :fraud_check_endpoint)
  @account_balance_endpoint Application.compile_env(
                              :ref_elixir_flow_pipeline,
                              :account_balance_endpoint
                            )

  def main(args \\ []) do
    Logger.info("Starting RefElixir.BasicPipeline")
    Logger.info("Arguments: #{args}")

    args
    |> parse_args()
    |> _response()
  end

  defp parse_args(args) do
    {opts, file_path, _} =
      args
      |> OptionParser.parse(switches: [file: :boolean])

    {opts, List.to_string(file_path)}
  end

  defp _response({opts, file_path}) do
    case opts[:file] do
      true ->
        Logger.info("File path: #{inspect(file_path)}")
        file_path

      _ ->
        Logger.error("
          File path is not specified
          usage:
            ./ref_elixir_basic_pipeline {options} arg1 arg2 ...
          ")
        raise "Error"
    end
  end

  def process_flow_benchmark(file_path) do
    Benchee.run(
      %{
        "process_data" => fn ->
          process_flow(file_path)
        end
      },
      warmup: 0,
      time: 3,
      # memory_time: 2,
      # parallel: 1,
      formatters: [
        {Benchee.Formatters.HTML, file: "benchmark/process_flow.html"},
        Benchee.Formatters.Console
      ],
      print: [
        benchmarking: true,
        configuration: true,
        fast_warning: true
      ]
    )
  end

  def process_flow(file_path) do
    read_feed_files(file_path)
    |> Flow.from_enumerables(max_demand: 10)
    |> Flow.partition(max_demand: 10, stages: 5)
    |> Flow.map(&parse_record/1)
    |> Flow.partition(max_demand: 10, stages: 5)
    |> Flow.map(&experian_check/1)
    |> Flow.partition(max_demand: 10, stages: 5)
    |> Flow.map(&equifax_check/1)
    |> Flow.partition(max_demand: 10, stages: 5)
    |> Flow.map(&aml_check/1)
    |> Flow.partition(max_demand: 10, stages: 5)
    |> Flow.map(&fraud_check/1)
    |> Flow.partition(max_demand: 10, stages: 5)
    |> Flow.map(&account_balance_check/1)
    |> Flow.partition(max_demand: 10, stages: 5)
    |> Flow.map(&credit_decision/1)
    |> Flow.map(&print_record/1)
    |> Flow.run()
  end

  def read_feed_files(file_path) do
    file_streams =
      for file <- File.ls!(file_path) do
        File.stream!(file_path <> "/" <> file, read_ahead: 100_000)
        |> Stream.drop(1)
      end

    file_streams
  end

  def parse_record(row) do
    Logger.info("Parsing record: #{row}")
    [row] = CSV.parse_string(row, skip_headers: false)

    %{
      request_id: Enum.at(row, 0),
      name: Enum.at(row, 1),
      credit_requested: Enum.at(row, 2),
      requested_date: Enum.at(row, 3),
      location: Enum.at(row, 4),
      status: "NEW_REQUEST",
      activity_log: ["REQUEST_CREATED"]
    }
  end

  def print_record(record) do
    Logger.info("
      request_id: #{inspect(record.request_id)}
      name: #{inspect(record.name)}
      credit_requested: #{inspect(record[:credit_requested])}
      requested_date: #{inspect(record[:requested_date])}
      location: #{inspect(record[:location])}
      status: #{inspect(record[:status])}
      activity_log: #{inspect(record[:activity_log])}
      ")
  end

  def experian_check(request) do
    Logger.info("Request for Experian Check: #{inspect(request.request_id)}")

    _response =
      case HTTPoison.get!(@experian_endpoint) do
        %HTTPoison.Response{status_code: 200} ->
          %{
            request_id: request.request_id,
            request_type: "Experian Check",
            status_code: 200
          }

        _ ->
          %{
            request_id: request.request_id,
            request_type: "Experian Check",
            status_code: :error
          }
      end

    # Logger.info("Response Experian Check: #{inspect(_response)}")

    request_processed = %{
      request
      | status: "EXPERIAN_CHECK_DONE",
        activity_log: Enum.concat(request.activity_log, ["EXPERIAN_CHECK"])
    }

    request_processed
  end

  def equifax_check(request) do
    Logger.info("Request for Equifax Check: #{inspect(request.request_id)}")

    _response =
      case HTTPoison.get!(@equifax_endpoint) do
        %HTTPoison.Response{status_code: 200} ->
          %{
            request_id: request.request_id,
            request_type: "Equifax Check",
            status_code: 200
          }

        _ ->
          %{
            request_id: request.request_id,
            request_type: "Equifax Check",
            status_code: :error
          }
      end

    # Logger.info("Response from Equifax Check: #{inspect(_response)}")

    request_processed = %{
      request
      | status: "EQUIFAX_CHECK_DONE",
        activity_log: Enum.concat(request.activity_log, ["EQUIFAX_CHECK"])
    }

    request_processed
  end

  def aml_check(request) do
    Logger.info("Request for AML Check: #{inspect(request.request_id)}")

    _response =
      case HTTPoison.get!(@aml_check_endpoint) do
        %HTTPoison.Response{status_code: 200} ->
          %{
            request_id: request.request_id,
            request_type: "AML Check",
            status_code: 200
          }

        _ ->
          %{
            request_id: request.request_id,
            request_type: "AML Check",
            status_code: :error
          }
      end

    # Logger.info("Response from AML Check: #{inspect(_response)}")

    request_processed = %{
      request
      | status: "AML_CHECK_DONE",
        activity_log: Enum.concat(request.activity_log, ["AML_CHECK"])
    }

    request_processed
  end

  def fraud_check(request) do
    Logger.info("Request for Fraud Check: #{inspect(request.request_id)}")

    _response =
      case HTTPoison.get!(@fraud_check_endpoint) do
        %HTTPoison.Response{status_code: 200} ->
          %{
            request_id: request.request_id,
            request_type: "Fraud Check",
            status_code: 200
          }

        _ ->
          %{
            request_id: request.request_id,
            request_type: "Fraud Check",
            status_code: :error
          }
      end

    # Logger.info("Response from Fraud Check: #{inspect(_response)}")

    request_processed = %{
      request
      | status: "FRAUD_CHECK_DONE",
        activity_log: Enum.concat(request.activity_log, ["FRAUD_CHECK"])
    }

    request_processed
  end

  def account_balance_check(request) do
    Logger.info("Request for Account Balance Check: #{inspect(request.request_id)}")

    _response =
      case HTTPoison.get!(@account_balance_endpoint) do
        %HTTPoison.Response{status_code: 200} ->
          %{
            request_id: request.request_id,
            request_type: "Account Balance Check",
            status_code: 200
          }

        _ ->
          %{
            request_id: request.request_id,
            request_type: "Account Balance Check",
            status_code: :error
          }
      end

    # Logger.info("Response from Account Balance Check: #{inspect(_response)}")

    request_processed = %{
      request
      | status: "ACCOUNT_BALANCE_CHECK_DONE",
        activity_log: Enum.concat(request.activity_log, ["ACCOUNT_BALANCE_CHECK"])
    }

    request_processed
  end

  def credit_decision(request) do
    Logger.info("Final decision on credit request: #{inspect(request.request_id)}")

    {credit_requested, _} = Integer.parse(request.credit_requested)
    Logger.info("Credit requested: #{inspect(credit_requested)}")

    final_decision =
      case {request.status, credit_requested} do
        {"ACCOUNT_BALANCE_CHECK_DONE", _}
        when credit_requested > 1_000_000 ->
          "REJECTED"

        {"ACCOUNT_BALANCE_CHECK_DONE", _} ->
          "EXCEPTION_REVIEW"

        _ ->
          "REJECTED"
      end

    # Logger.info("Final decision: #{inspect(final_decision)}")

    request_processed = %{
      request
      | status: final_decision,
        activity_log: Enum.concat(request.activity_log, ["CREDIT_DECISION"])
    }

    request_processed
  end
end

# Commands
# RefElixir.FlowPipeline.process_flow("/Users/rajesh/Learn/elixir/ref_elixir_flow_pipeline/priv/data")
# RefElixir.FlowPipeline.process_flow_benchmark("/Users/rajesh/Learn/elixir/ref_elixir_flow_pipeline/priv/data")
