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
    assert terminate_metadata.subscriptions == []
    assert is_atom(terminate_metadata.reason) or is_binary(terminate_metadata.reason)
    refute Map.has_key?(terminate_metadata, :socket_subscription_count)
  end

  test "includes orphaned subscriptions in transport terminate metadata" do
    assert {:ok, client} = Test.Client.start()

    assert_receive {:telemetry, :init, _, _, _}

    :ok = Test.Client.push(client, %{type: "connection_init"})
    assert {:ok, [{:text, _}]} = Test.Client.get_new_replies(client)

    id = "orphaned-subscription"

    :ok =
      Test.Client.push(client, %{
        id: id,
        type: "subscribe",
        payload: %{
          query: """
          subscription ThingChanges($id: Int!) {
            thing_changes(id: $id) {
              id
              name
            }
          }
          """,
          variables: %{id: 2}
        }
      })

    assert {:ok, []} = Test.Client.get_new_replies(client)

    :ok = Test.Client.close(client)

    assert_receive {
      :telemetry,
      :terminate,
      [:absinthe_graphql_ws, :transport, :terminate],
      %{socket_subscription_count: 1},
      terminate_metadata
    }

    assert [%{id: ^id, operation_name: nil}] = terminate_metadata.subscriptions
  end
end
