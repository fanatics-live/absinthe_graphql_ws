defmodule Absinthe.GraphqlWS.ConnectionInitTelemetryTest do
  use ExUnit.Case

  @handler_id {__MODULE__, :connection_init}

  setup context do
    parent = self()

    :ok =
      :telemetry.attach(
        @handler_id,
        [:absinthe_graphql_ws, :handle_inbound, :connection_init],
        fn event, measurements, metadata, _config ->
          send(parent, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(@handler_id) end)

    assert {:ok, client} = Test.Client.start()
    on_exit(fn -> Test.Client.close(client) end)

    Map.merge(context, %{client: client})
  end

  test "emits lifecycle telemetry on successful connection_init", %{client: client} do
    :ok = Test.Client.push(client, %{type: "connection_init"})

    assert_receive {
      :telemetry,
      [:absinthe_graphql_ws, :handle_inbound, :connection_init],
      measurements,
      metadata
    }

    assert measurements == %{socket_subscription_count: 0}
    assert metadata.platform == "unknown"
    refute Map.has_key?(metadata, :socket_subscription_count)
  end
end
