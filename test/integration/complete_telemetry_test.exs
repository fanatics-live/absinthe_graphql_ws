defmodule Absinthe.GraphqlWS.CompleteTelemetryTest do
  use ExUnit.Case

  @handler_id {__MODULE__, :complete_span}

  setup context do
    events = [
      [:absinthe_graphql_ws, :handle_inbound, :complete, :start],
      [:absinthe_graphql_ws, :handle_inbound, :complete, :stop]
    ]

    parent = self()

    :ok =
      :telemetry.attach_many(
        @handler_id,
        events,
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

  test "emits a telemetry span around unsubscribe with process and subscription metrics", %{
    client: client
  } do
    id = "subscription-to-unsubscribe"

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

    :ok = Test.Client.push(client, %{id: id, type: "complete"})

    assert_receive {:telemetry, [:absinthe_graphql_ws, :handle_inbound, :complete, :start], start_measurements,
                    start_metadata}

    assert Map.has_key?(start_measurements, :monotonic_time)
    assert Map.has_key?(start_measurements, :system_time)

    assert start_metadata.id == id
    assert start_metadata.socket_subscription_count == 1
    assert start_metadata.pdict_subscription_count == 1
    assert start_metadata.field_keys_unregistered == 1
    assert start_metadata.subscription_field_key_count == 1
    assert is_integer(start_metadata.memory_before)
    assert is_integer(start_metadata.message_queue_len_before)

    assert_receive {:telemetry, [:absinthe_graphql_ws, :handle_inbound, :complete, :stop], stop_measurements,
                    stop_metadata}

    assert Map.has_key?(stop_measurements, :duration)
    assert is_integer(stop_measurements.duration)

    assert stop_metadata.id == id
    assert stop_metadata.socket_subscription_count == 1
    assert stop_metadata.field_keys_unregistered == 1
    assert is_integer(stop_metadata.memory_before)
    assert is_integer(stop_metadata.memory_after)
    assert is_integer(stop_metadata.memory_delta)
    assert stop_metadata.memory_delta == stop_metadata.memory_after - stop_metadata.memory_before
    assert is_integer(stop_metadata.message_queue_len_before)
    assert is_integer(stop_metadata.message_queue_len_after)
  end
end
