defmodule Absinthe.GraphqlWS.SocketTest do
  use ExUnit.Case

  alias Test.Site.Endpoint

  @subscription_subscribe_event [:absinthe_graphql_ws, :subscription, :subscribe]
  @subscription_unsubscribe_event [:absinthe_graphql_ws, :subscription, :unsubscribe]
  @subscription_unsubscribe_duration_stop_event [
    :absinthe_graphql_ws,
    :subscription,
    :unsubscribe,
    :duration,
    :stop
  ]

  defp setup_client(_context) do
    assert {:ok, client} = Test.Client.start()
    on_exit(fn -> Test.Client.close(client) end)
    [client: client]
  end

  def send_connection_init(%{client: client}) do
    :ok = Test.Client.push(client, %{type: "connection_init"})

    assert {:ok,
            [
              {:text, Jason.encode!(%{"type" => "connection_ack", "payload" => %{}})}
            ]} ==
             Test.Client.get_new_replies(client)

    :ok
  end

  def assert_connected(client) do
    assert Test.Client.connected?(client)
  end

  def assert_socket_closed(client, code, payload) do
    assert {:ok, [{:close, ^code, ^payload}]} = Test.Client.get_new_replies(client)

    Test.Retry.retry_for(5000, fn ->
      refute Test.Client.connected?(client)
    end)
  end

  defp assert_json_received(client, payload) do
    assert {:ok, [{:text, json}]} = Test.Client.get_new_replies(client)
    assert json == Jason.encode!(payload)
  end

  defp attach_telemetry(test, event_name) do
    handler_id = {__MODULE__, test, event_name}
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        event_name,
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry_event, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)
  end

  describe "initalization" do
    test "starts and stops" do
      assert {:ok, client} = Test.Client.start()
      Test.Client.close(client)
    end
  end

  describe "on ConnectionInit" do
    setup :setup_client

    test "replies with ConnectionAck", %{client: client} do
      :ok = Test.Client.push(client, %{type: "connection_init"})

      assert_json_received(client, %{
        "payload" => %{},
        "type" => "connection_ack"
      })
    end

    test "closes the socket with an error if received multiple times", %{client: client} do
      :ok = Test.Client.push(client, %{type: "connection_init"})
      assert {:ok, [{:text, _}]} = Test.Client.get_new_replies(client)

      assert_connected(client)

      :ok = Test.Client.push(client, %{type: "connection_init"})
      assert_socket_closed(client, 4429, "Too many initialisation requests")
    end
  end

  describe "on Ping" do
    setup [:setup_client, :send_connection_init]

    test "replies with Pong", %{client: client} do
      :ok = Test.Client.push(client, %{type: "ping", payload: %{}})

      assert_json_received(client, %{
        "payload" => %{},
        "type" => "pong"
      })
    end
  end

  describe "on message receipt that does not follow the spec" do
    setup [:setup_client, :send_connection_init]

    test "closes the socket with 4400", %{client: client} do
      :ok = Test.Client.push(client, %{type: "made_up"})
      assert_socket_closed(client, 4400, "Unhandled message from client")
    end
  end

  describe "on Subscribe if ConnectionInit has not been sent" do
    setup :setup_client

    test "closes the socket with 4400", %{client: client} do
      id = "query-before-init"

      query = """
      query {
        things {
          name
        }
      }
      """

      :ok = Test.Client.push(client, %{id: id, type: "subscribe", payload: %{query: query}})
      assert_socket_closed(client, 4400, "Subscribe message received before ConnectionInit")
    end
  end

  describe "on Subscribe with a query" do
    setup [:setup_client, :send_connection_init]

    test "passes the query to Absinthe and responds with Next + Complete", %{client: client} do
      id = "simple query"

      query = """
      query {
        things {
          name
        }
      }
      """

      :ok = Test.Client.push(client, %{id: id, type: "subscribe", payload: %{query: query}})

      assert_json_received(client, %{
        "payload" => %{
          "data" => %{
            "things" => [
              %{"name" => "one"},
              %{"name" => "two"}
            ]
          }
        },
        "type" => "next",
        "id" => id
      })

      assert_json_received(client, %{
        "payload" => %{},
        "type" => "complete",
        "id" => id
      })
    end
  end

  describe "on Subscribe with a mutation" do
    setup [:setup_client, :send_connection_init]

    test "passes variables to Absinthe", %{client: client} do
      id = "mutation-with-variables"

      :ok =
        Test.Client.push(client, %{
          id: id,
          type: "subscribe",
          payload: %{
            query: """
            mutation ChangeThing($id: Int! $name: String!) {
              change_thing(id: $id, name: $name) {
                id
                name
              }
            }
            """,
            variables: %{id: 1, name: "another one"}
          }
        })

      assert_json_received(client, %{
        "payload" => %{
          "data" => %{
            "change_thing" => %{"id" => 1, "name" => "another one"}
          }
        },
        "type" => "next",
        "id" => id
      })

      assert_json_received(client, %{
        "payload" => %{},
        "type" => "complete",
        "id" => id
      })
    end
  end

  describe "on Subscribe with a subscription" do
    setup [:setup_client, :send_connection_init]

    test "emits telemetry when a subscription is accepted", %{client: client, test: test} do
      attach_telemetry(test, @subscription_subscribe_event)

      id = "subscription"

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

      assert_receive {:telemetry_event, @subscription_subscribe_event,
                      %{field_key_count: 1, socket_subscription_count: 1},
                      %{
                        operation_name: nil,
                        platform: "unknown",
                        route: "/graphql",
                        subscription_field: :thing_changes,
                        subscription_fields: [:thing_changes]
                      } = metadata}

      refute Map.has_key?(metadata, :topic)
      refute Map.has_key?(metadata, :query)
      refute Map.has_key?(metadata, :variables)
      refute Map.has_key?(metadata, :user_id)
      refute Map.has_key?(metadata, :socket_id)
    end

    test "pushes Next messages for the subscription topic, as they are published", %{client: client} do
      id = "subscription"

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

      Absinthe.Subscription.publish(Endpoint, %{name: "blue"}, thing_changes: "2")

      assert_json_received(client, %{
        "payload" => %{
          "data" => %{"thing_changes" => %{"id" => 2, "name" => "blue"}}
        },
        "type" => "next",
        "id" => id
      })

      Absinthe.Subscription.publish(Endpoint, %{name: "fun"}, thing_changes: "1")

      assert {:ok, []} = Test.Client.get_new_replies(client)
    end

    test "stops a subscription when client sends a Complete", %{client: client, test: test} do
      attach_telemetry(test, @subscription_unsubscribe_event)
      attach_telemetry({test, :unsubscribe_stop}, @subscription_unsubscribe_duration_stop_event)

      id = "subscription-to-be-cancelled"

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
      Absinthe.Subscription.publish(Endpoint, %{name: "blue"}, thing_changes: "2")

      assert_json_received(client, %{
        "payload" => %{
          "data" => %{"thing_changes" => %{"id" => 2, "name" => "blue"}}
        },
        "type" => "next",
        "id" => id
      })

      :ok = Test.Client.push(client, %{id: id, type: "complete"})
      assert {:ok, []} = Test.Client.get_new_replies(client)

      assert_receive {:telemetry_event, @subscription_unsubscribe_event,
                      %{field_key_count: 1, socket_subscription_count: 1},
                      %{
                        operation_name: nil,
                        platform: "unknown",
                        reason: :client_complete,
                        route: "/graphql",
                        subscription_field: :thing_changes,
                        subscription_fields: [:thing_changes]
                      }}

      assert_receive {:telemetry_event, @subscription_unsubscribe_duration_stop_event, %{duration: duration},
                      %{after_memory: after_memory, after_message_queue_len: after_message_queue_len}}

      assert is_integer(duration)
      assert is_integer(after_memory)
      assert is_integer(after_message_queue_len)

      Absinthe.Subscription.publish(Endpoint, %{name: "true"}, thing_changes: "2")
      assert {:ok, []} = Test.Client.get_new_replies(client)
    end

    test "emits unsubscribe telemetry for active subscriptions when socket terminates" do
      assert {:ok, client} = Test.Client.start()
      send_connection_init(%{client: client})
      attach_telemetry(:terminate_cleanup, @subscription_unsubscribe_event)

      id = "subscription-cleanup"

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
      Test.Client.close(client)

      assert_receive {:telemetry_event, @subscription_unsubscribe_event,
                      %{field_key_count: 1, socket_subscription_count: 1},
                      %{
                        operation_name: nil,
                        platform: "unknown",
                        reason: :socket_terminate,
                        route: "/graphql",
                        subscription_field: :thing_changes,
                        subscription_fields: [:thing_changes]
                      }}
    end

    test "correct error payload for subscription failure", %{client: client, test: test} do
      attach_telemetry(test, @subscription_subscribe_event)

      id = "subscription-with-error"

      :ok =
        Test.Client.push(client, %{
          id: id,
          type: "subscribe",
          payload: %{
            query: """
            subscription HandleError {
              handleError {
                id
                name
              }
            }
            """
          }
        })

      assert_json_received(
        client,
        %{
          "id" => "subscription-with-error",
          "payload" => [%{"locations" => [%{"column" => 3, "line" => 2}], "message" => "subscribe error"}],
          "type" => "error"
        }
      )

      refute_receive {:telemetry_event, @subscription_subscribe_event, _, _}
    end
  end

  describe "handle_message callbacks" do
    setup [:setup_client, :send_connection_init]

    test "are called when the socket receives &handle_info/2 with a message not caught by graphql-ws", %{client: client} do
      id = "handle-message-callback"

      Test.Site.TestPubSub.subscribe(:handle_message_callback)

      :ok =
        Test.Client.push(client, %{
          id: id,
          type: "subscribe",
          payload: %{
            query: """
            subscription HandleMessage($subscriptionParam: String!) {
              handle_message(subscriptionParam: $subscriptionParam)
            }
            """,
            variables: %{subscriptionParam: "boo"}
          }
        })

      assert {:ok, []} = Test.Client.get_new_replies(client)

      assert_receive({:subscription, %{}} = reply)
      assert reply == {:subscription, %{subscription_param: "boo"}}
    end
  end
end
