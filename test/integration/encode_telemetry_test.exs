defmodule Absinthe.GraphqlWS.EncodeTelemetryTest do
  use ExUnit.Case

  @handler_id {__MODULE__, :encode}

  setup do
    parent = self()

    :ok =
      :telemetry.attach(
        @handler_id,
        [:absinthe_graphql_ws, :encode, :stop],
        fn event, measurements, metadata, _config -> send(parent, {:telemetry, event, measurements, metadata}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(@handler_id) end)

    assert {:ok, client} = Test.Client.start()
    on_exit(fn -> Test.Client.close(client) end)

    :ok = Test.Client.push(client, %{type: "connection_init"})
    assert {:ok, [{:text, _}]} = Test.Client.get_new_replies(client)

    %{client: client}
  end

  test "emits an encode span with duration and byte size", %{client: client} do
    id = "encode-query"
    query = "query Things { things { id name } }"

    :ok = Test.Client.push(client, %{id: id, type: "subscribe", payload: %{query: query, operationName: "Things"}})
    assert {:ok, [{:text, next} | _]} = Test.Client.get_new_replies(client)

    assert_receive {:telemetry, [:absinthe_graphql_ws, :encode, :stop], measurements, metadata}
    assert is_integer(measurements.duration)
    assert measurements.byte_size == byte_size(next)
    assert %{type: :next, id: ^id, operation_name: "Things"} = metadata
  end
end
