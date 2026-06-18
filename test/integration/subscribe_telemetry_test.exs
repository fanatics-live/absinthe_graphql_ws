defmodule Absinthe.GraphqlWS.SubscribeTelemetryTest do
  use ExUnit.Case

  @handler_id {__MODULE__, :subscribe}

  setup context do
    parent = self()

    :ok =
      :telemetry.attach(
        @handler_id,
        [:absinthe_graphql_ws, :handle_inbound, :subscribe],
        fn event, measurements, metadata, _config ->
          send(parent, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(@handler_id) end)

    assert {:ok, client} = Test.Client.start()
    on_exit(fn -> Test.Client.close(client) end)

    :ok = Test.Client.push(client, %{type: "connection_init"})
    assert {:ok, [{:text, _}]} = Test.Client.get_new_replies(client)

    Map.merge(context, %{client: client})
  end

  test "emits socket_subscription_count in measurements after subscribe", %{client: client} do
    id = "subscription-count-test"

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

    assert_receive {
      :telemetry,
      [:absinthe_graphql_ws, :handle_inbound, :subscribe],
      measurements,
      metadata
    }

    assert measurements == %{socket_subscription_count: 1}
    assert metadata.id == id
    refute Map.has_key?(metadata, :socket_subscription_count)
  end
end
