defmodule Absinthe.GraphqlWS.Transport do
  @moduledoc """
  Handles messages coming into the socket from clients (implemented in `handle_in/2`)
  as well as messages coming from within Elixir/Absinthe (implemented in `handle_info/2`).

  If the optional `c:Absinthe.GraphqlWS.Socket.handle_message/2` callback is implemented on
  the socket, then messages that are not specifically caught by `handle_info/2` in this
  module will be passed through to `c:Absinthe.GraphqlWS.Socket.handle_message/2`.

  **Note:** This module is not intended for use by individuals integrating this library into
  their codebase, but is documented to help understand the intentions of the code.
  """

  alias Absinthe.GraphqlWS.Message
  alias Absinthe.GraphqlWS.Socket
  alias Absinthe.GraphqlWS.Util
  alias Phoenix.Socket.Broadcast

  require Logger

  @ping "ping"
  @pong "pong"
  @subscription_subscribe_event [:absinthe_graphql_ws, :subscription, :subscribe]
  @subscription_unsubscribe_event [:absinthe_graphql_ws, :subscription, :unsubscribe]
  @subscription_unsubscribe_duration_event [:absinthe_graphql_ws, :subscription, :unsubscribe, :duration]

  @type control :: Socket.control()
  @type reply_inbound() :: Socket.reply_inbound()
  @type reply_message() :: Socket.reply_message()
  @type socket() :: Socket.t()

  defmacrop debug(msg), do: quote(do: Logger.debug("[graph-socket@#{inspect(self())}] #{unquote(msg)}"))
  defmacrop warn(msg), do: quote(do: Logger.warning("[graph-socket@#{inspect(self())}] #{unquote(msg)}"))

  @doc """
  Generally this will only receive `:pong` messages in response to our keepalive
  ping messages. Client-side websocket libraries handle these control frames
  automatically in order to adhere to the spec, so unless a customer is writing their
  own low-level websocket it should be handled for them.
  """
  @spec handle_control({term(), opcode: control()}, socket()) :: reply_inbound()
  def handle_control({_, opcode: :ping}, socket), do: {:reply, :ok, {:pong, @pong}, socket}

  def handle_control({_, opcode: :pong}, socket) do
    start = socket.assigns[:start]
    measurements = %{duration: System.monotonic_time() - start}
    metadata = %{}
    :telemetry.execute([:absinthe_graphql_ws, :keepalive, :stop], measurements, metadata)
    system_time = System.system_time()
    socket = Util.assign(socket, last_inbound_pong: system_time, last_keepalive: system_time)

    {:ok, socket}
  end

  def handle_control(message, state) do
    warn(" unhandled control frame #{inspect(message)}")
    {:ok, state}
  end

  @doc """
  Receive messages from clients. We expect all incoming messages to be JSON encoded
  text, so if something else comes in we blow up.
  """
  @spec handle_in({binary(), [opcode: :text]}, socket()) :: reply_inbound()
  def handle_in({text, [opcode: :text]}, socket) do
    text
    |> Util.json_library().decode()
    |> case do
      {:ok, json} ->
        handle_inbound(json, socket)

      {:error, reason} ->
        warn("JSON parse error: #{inspect(reason)}")
        {:reply, :error, {:text, Message.Error.new("4400")}, socket}
    end
  end

  @doc """
  Receive messages from inside the house.

  * `:keepalive` - Regularly send messages with opcode of `0x09`, ie `:ping`. The `graphql-ws`
    library has a strong opinion that it does not want to implement client-side keepalive, so
    in order to keep the websocket from closing we need to send it messages.

  * `:gc` - Periodically trigger garbage collection on the socket process when `gc_interval` is
    configured. Emits telemetry events `[:absinthe_graphql_ws, :gc, :start]` and
    `[:absinthe_graphql_ws, :gc, :stop]` with process info before and after GC in the metadata.

  * `subscription:data` - After we subscribe to an Absinthe subscription, we may receive messages
    for the relevant subscription. The `graphql-ws` will have sent us an `id` along with the
    subscription query, so we need to map our internal topic back to that `id` in order for the
    client to figure out what to do with our message.

  * `:complete` - If we get a `query` or a `mutation` on the websocket, we're supposed to reply
    with a `Next` message followed by a `Complete` message. We follow through on the latter by
    putting a message on our process queue.

  * fallthrough - If `c:Absinthe.GraphqlWs.Socket.handle_message/2` is defined on the socket,
    then uncaught messages will be sent there.
  """
  @spec handle_info(term(), socket()) :: reply_message()
  def handle_info(:keepalive, socket) do
    Process.send_after(self(), :keepalive, socket.keepalive)
    start = System.monotonic_time()
    measurements = %{system_time: System.system_time()}
    metadata = %{}
    :telemetry.execute([:absinthe_graphql_ws, :keepalive, :start], measurements, metadata)
    system_time = System.system_time()
    socket = Util.assign(socket, start: start, last_outbound_ping: system_time, last_keepalive: system_time)

    {:push, {:ping, @ping}, socket}
  end

  def handle_info(:gc, socket) do
    before_info = self() |> Process.info() |> Map.new()

    :telemetry.span([:absinthe_graphql_ws, :gc], %{before: before_info}, fn ->
      :erlang.garbage_collect()
      after_info = self() |> Process.info() |> Map.new()

      {:ok, %{before: before_info, after: after_info}}
    end)

    maybe_schedule_gc(socket)

    {:ok, socket}
  end

  def handle_info(%Broadcast{event: "subscription:data", payload: payload, topic: topic}, socket) do
    {subscription_id, operation_name} = subscription_info(socket.subscriptions, topic)
    message = Message.Next.new(subscription_id, payload.result)
    measurements = %{payload_size: byte_size(message)}

    metadata = %{
      platform: get_platform(socket),
      session_id: get_session_id(socket),
      client_app_version: get_client_app_version(socket),
      user_id: get_user_id(socket),
      payload: payload,
      operation_name: operation_name
    }

    :telemetry.execute([:absinthe_graphql_ws, :handle_info, :broadcast], measurements, metadata)

    {:push, {:text, message}, socket}
  end

  def handle_info({:complete, id}, socket) do
    {:push, {:text, Message.Complete.new(id)}, socket}
  end

  def handle_info(message, socket) do
    if function_exported?(socket.handler, :handle_message, 2) do
      socket.handler.handle_message(message, socket)
    else
      {:ok, socket}
    end
  end

  @doc """
  Process was stopped.
  """
  @spec terminate(term(), socket()) :: :ok
  def terminate(reason, socket) do
    Enum.each(socket.subscriptions, fn {topic, subscription} ->
      emit_subscription_unsubscribe(topic, socket, subscription, :socket_terminate, %{
        field_key_count: field_key_count(topic, socket),
        socket_subscription_count: map_size(socket.subscriptions)
      })
    end)

    debug("terminated: #{inspect(reason)}")
    :ok
  end

  @doc """
  Callbacks for parsed JSON payloads coming in from a client.

  See:
  https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md
  """
  @spec handle_inbound(map(), socket()) :: reply_inbound()
  def handle_inbound(%{"type" => "connection_init"}, %{initialized?: true} = socket) do
    metadata = %{
      code: 4429,
      operation: :connection_init,
      reason: :too_many_initialisation_requests,
      platform: get_platform(socket)
    }

    :telemetry.execute([:absinthe_graphql_ws, :handle_inbound, :error], %{}, metadata)

    close(4429, "Too many initialisation requests", socket)
  end

  def handle_inbound(%{"type" => "connection_init"} = message, %{handler: handler} = socket) do
    if function_exported?(handler, :handle_init, 2) do
      case handler.handle_init(Map.get(message, "payload", %{}), socket) do
        {:ok, payload, socket} ->
          socket = %{socket | initialized?: true}
          maybe_schedule_gc(socket)
          {:reply, :ok, {:text, Message.ConnectionAck.new(payload)}, socket}

        {:error, payload, socket} ->
          {:reply, :ok, {:text, Message.Error.new(payload)}, socket}
      end
    else
      socket = %{socket | initialized?: true}
      maybe_schedule_gc(socket)
      {:reply, :ok, {:text, Message.ConnectionAck.new()}, socket}
    end
  end

  def handle_inbound(%{"type" => "subscribe"}, %{initialized?: false} = socket) do
    metadata = %{
      code: 4400,
      operation: :subscribe,
      reason: :subscribe_before_connection_init,
      platform: get_platform(socket)
    }

    :telemetry.execute([:absinthe_graphql_ws, :handle_inbound, :error], %{}, metadata)

    close(4400, "Subscribe message received before ConnectionInit", socket)
  end

  def handle_inbound(%{"id" => id, "type" => "subscribe", "payload" => payload}, socket) do
    handle_subscribe(payload, id, socket)
  end

  def handle_inbound(%{"id" => id, "type" => "complete"}, socket) do
    socket.subscriptions
    |> Enum.find_value(fn
      {topic, %{id: ^id}} ->
        {:ok, topic}

      {topic, ^id} ->
        {:ok, topic}

      _ ->
        false
    end)
    |> case do
      {:ok, topic} ->
        debug("unsubscribing from topic #{topic}")
        subscription = Map.fetch!(socket.subscriptions, topic)
        field_key_count = field_key_count(topic, socket)
        socket_subscription_count = map_size(socket.subscriptions)
        unsubscribe_metadata = subscription_metadata(topic, socket, subscription, :client_complete)

        unsubscribe_start_metadata =
          Map.merge(unsubscribe_metadata, process_stats(:before))

        :telemetry.span(@subscription_unsubscribe_duration_event, unsubscribe_start_metadata, fn ->
          Phoenix.PubSub.unsubscribe(socket.pubsub, topic)
          Absinthe.Subscription.unsubscribe(socket.endpoint, topic)

          measurements = %{
            field_key_count: field_key_count,
            socket_subscription_count: socket_subscription_count
          }

          emit_subscription_unsubscribe(measurements, unsubscribe_metadata)

          {:ok, Map.merge(unsubscribe_metadata, process_stats(:after))}
        end)

        {:ok, %{socket | subscriptions: Map.delete(socket.subscriptions, topic)}}

      _ ->
        {:ok, socket}
    end
  end

  def handle_inbound(%{"type" => "ping"}, socket) do
    system_time = System.system_time()
    message = Message.Pong.new()
    measurements = %{payload_size: byte_size(message)}
    metadata = %{platform: get_platform(socket)}
    :telemetry.execute([:absinthe_graphql_ws, :handle_inbound, :ping], measurements, metadata)

    socket = Util.assign(socket, last_inbound_ping: system_time, last_keepalive: system_time)
    {:reply, :ok, {:text, message}, socket}
  end

  def handle_inbound(msg, socket) do
    warn("unhandled message #{inspect(msg)}")
    close(4400, "Unhandled message from client", socket)
  end

  @doc """
  Subscribe messages in graphql-ws may include a subscription, implying a subscription to
  a long term stream of data. These messages may also be queries or mutations, so do not require
  a stream.
  """
  def handle_subscribe(payload, id, socket) do
    with %{schema: schema} <- socket.absinthe,
         {:ok, variables} <- parse_variables(payload),
         {:ok, query} <- parse_query(payload) do
      operation_name = parse_operation_name(payload)

      opts =
        socket.absinthe.opts
        |> Keyword.put(:variables, variables)
        |> maybe_put_operation_name(operation_name)

      Absinthe.Logger.log_run(:debug, {
        query,
        schema,
        [],
        opts
      })

      run_doc(socket, id, query, socket.absinthe, opts, operation_name)
    else
      _ ->
        {:ok, socket}
    end
  end

  defp close(code, message, socket) do
    {:reply, :ok, {:close, code, message}, socket}
  end

  defp maybe_schedule_gc(%{assigns: %{gc_interval: gc_interval}}) when is_integer(gc_interval) and gc_interval > 0 do
    Process.send_after(self(), :gc, gc_interval)
  end

  defp maybe_schedule_gc(_socket), do: :ok

  defp parse_query(%{"query" => query}) when is_binary(query), do: {:ok, query}
  defp parse_query(_), do: {:ok, ""}

  defp parse_variables(%{"variables" => variables}) when is_map(variables), do: {:ok, variables}
  defp parse_variables(_), do: {:ok, %{}}

  defp parse_operation_name(%{"operationName" => name}) when is_binary(name) and name != "" do
    name
  end

  defp parse_operation_name(_), do: nil

  def pipeline(schema, options) do
    Absinthe.Pipeline.for_document(schema, options)
  end

  defp run_doc(socket, id, query, config, opts, operation_name) do
    case run(query, config[:schema], config[:pipeline], opts) do
      {:ok, %{"subscribed" => topic}, context} ->
        debug("subscribed to topic #{topic}")

        :ok =
          Phoenix.PubSub.subscribe(
            socket.pubsub,
            topic,
            # metadata: {:fastlane, self(), @serializer, []},
            link: true
          )

        socket = merge_opts(socket, context: context)

        subscription = %{
          id: id,
          operation_name: operation_name
        }

        measurements = %{
          field_key_count: field_key_count(topic, socket),
          socket_subscription_count: map_size(socket.subscriptions) + 1
        }

        :telemetry.execute(
          @subscription_subscribe_event,
          measurements,
          subscription_metadata(topic, socket, subscription)
        )

        {:ok, %{socket | subscriptions: Map.put(socket.subscriptions, topic, subscription)}}

      {:ok, %{data: _} = reply, context} ->
        queue_complete_message(id)
        socket = merge_opts(socket, context: context)
        {:reply, :ok, {:text, Message.Next.new(id, reply)}, socket}

      {:ok, %{errors: errors}, context} ->
        socket = merge_opts(socket, context: context)
        {:reply, :ok, {:text, Message.Error.new(id, errors)}, socket}

      {:error, reply} ->
        {:reply, :error, {:text, Message.Error.new(id, reply)}, socket}
    end
  end

  defp run(document, schema, pipeline, options) do
    {module, fun} = pipeline

    case Absinthe.Pipeline.run(document, apply(module, fun, [schema, options])) do
      {:ok, %{result: result, execution: res}, _phases} ->
        {:ok, result, res.context}

      {:error, msg, _phases} ->
        {:error, msg}
    end
  end

  defp merge_opts(socket, opts) do
    %{socket | absinthe: %{socket.absinthe | opts: opts}}
  end

  defp maybe_put_operation_name(opts, nil), do: opts
  defp maybe_put_operation_name(opts, name), do: Keyword.put(opts, :operation_name, name)

  defp subscription_info(subscriptions, topic) do
    case Map.get(subscriptions, topic) do
      %{id: id, operation_name: operation_name} -> {id, operation_name}
      id -> {id, nil}
    end
  end

  defp emit_subscription_unsubscribe(topic, socket, subscription, reason, measurements) do
    emit_subscription_unsubscribe(measurements, subscription_metadata(topic, socket, subscription, reason))
  end

  defp emit_subscription_unsubscribe(measurements, metadata) do
    :telemetry.execute(
      @subscription_unsubscribe_event,
      measurements,
      metadata
    )
  end

  defp subscription_metadata(topic, socket, subscription, reason \\ nil) do
    metadata = %{
      operation_name: operation_name(subscription),
      platform: get_platform(socket),
      route: get_route(socket),
      subscription_field: subscription_field(topic, socket),
      subscription_fields: subscription_fields(topic, socket)
    }

    if is_nil(reason), do: metadata, else: Map.put(metadata, :reason, reason)
  end

  defp subscription_fields(topic, socket) do
    topic
    |> field_keys(socket)
    |> Enum.map(&field_key_name/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp subscription_field(topic, socket) do
    case subscription_fields(topic, socket) do
      [field] -> field
      [] -> :unknown
      _ -> :multiple
    end
  end

  defp field_key_count(topic, socket), do: topic |> field_keys(socket) |> length()

  defp field_keys(topic, socket) do
    case Process.get({Absinthe.Subscription, topic}, []) do
      [] -> registry_field_keys(socket.endpoint, topic)
      field_keys -> field_keys
    end
  end

  defp registry_field_keys(endpoint, topic) do
    registry = Absinthe.Subscription.registry_name(endpoint)
    self = self()

    for {^self, field_key} <- Registry.lookup(registry, {self, topic}) do
      field_key
    end
  rescue
    _ -> []
  end

  defp field_key_name({field, _topic}) when is_atom(field), do: field
  defp field_key_name(_field_key), do: nil

  defp operation_name(%{operation_name: operation_name}) when is_binary(operation_name) and operation_name != "" do
    operation_name
  end

  defp operation_name(_subscription), do: nil

  defp process_stats(prefix) do
    self()
    |> Process.info([:memory, :message_queue_len])
    |> Map.new()
    |> Map.new(fn {key, value} -> {:"#{prefix}_#{key}", value} end)
  end

  defp queue_complete_message(id), do: send(self(), {:complete, id})

  defp get_platform(socket), do: socket.assigns[:platform] || "unknown"

  defp get_route(%{connect_info: %{uri: %URI{path: path}}}) when is_binary(path), do: path

  defp get_route(%{connect_info: %{uri: uri}}) when is_binary(uri) do
    uri
    |> URI.parse()
    |> Map.get(:path)
    |> case do
      path when is_binary(path) and path != "" -> path
      _ -> "unknown"
    end
  end

  defp get_route(_socket), do: "unknown"

  defp get_session_id(socket), do: socket.assigns[:session_id]

  defp get_client_app_version(socket), do: socket.assigns[:client_app_version]

  defp get_user_id(socket), do: socket.assigns[:user_id]
end
