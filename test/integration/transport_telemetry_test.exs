defmodule Absinthe.GraphqlWS.TransportTelemetryTest do
  use ExUnit.Case

  @init_handler_id {__MODULE__, :transport_init}
  @terminate_handler_id {__MODULE__, :transport_terminate}

  setup context do
    parent = self()

    :ok =
      :telemetry.attach(
        @init_handler_id,
        [:absinthe_graphql_ws, :transport, :init],
        fn event, measurements, metadata, _config ->
          send(parent, {:telemetry, :init, event, measurements, metadata})
        end,
        nil
      )

    :ok =
      :telemetry.attach(
        @terminate_handler_id,
        [:absinthe_graphql_ws, :transport, :terminate],
        fn event, measurements, metadata, _config ->
          send(parent, {:telemetry, :terminate, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn ->
      :telemetry.detach(@init_handler_id)
      :telemetry.detach(@terminate_handler_id)
    end)

    context
  end

  test "emits transport init and terminate lifecycle telemetry" do
    assert {:ok, client} = Test.Client.start()

    assert_receive {
      :telemetry,
      :init,
      [:absinthe_graphql_ws, :transport, :init],
      init_measurements,
      init_metadata
    }

    assert init_measurements == %{}
    assert init_metadata.platform == "unknown"

    :ok = Test.Client.close(client)

    assert_receive {
      :telemetry,
      :terminate,
      [:absinthe_graphql_ws, :transport, :terminate],
      terminate_measurements,
      terminate_metadata
    }

    assert terminate_measurements == %{socket_subscription_count: 0}
    assert terminate_metadata.platform == "unknown"
    assert is_atom(terminate_metadata.reason) or is_binary(terminate_metadata.reason)
    refute Map.has_key?(terminate_metadata, :socket_subscription_count)
  end
end
