# AbsintheGraphqlWS

Adds a websocket transport for the [GraphQL over WebSocket
Protocol](https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md) to Absinthe running in
Phoenix.

See the [hex docs](https://hexdocs.pm/absinthe_graphql_ws) for more information.

## References

- https://github.com/enisdenjo/graphql-ws
- This project is heavily inspired by [subscriptions-transport-ws](https://github.com/maartenvanvliet/subscriptions-transport-ws)

## Installation

Add `absinthe_graphql_ws` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:absinthe_graphql_ws, "~> 0.3"}
  ]
end
```

## Usage

### Using the websocket client

```elixir
defmodule ExampleWeb.ApiClient do
  use GenServer

  alias Absinthe.GraphqlWS.Client

  def start(endpoint) do
    Client.start(endpoint)
  end

  def init(args) do
    {:ok, args}
  end

  def stop(client) do
    Client.close(client)
  end

  @gql """
  mutation ChangeSomething($id: String!) {
    changeSomething(id: $id) {
      id
      name
    }
  }
  """
  def change_something(client, thing_id) do
    {:ok, body} = Client.query(client, @gql, id: thing_id)

    case get_in(body, ~w[data changeSomething]) do
      nil -> {:error, get_in(body, ~w[errors])}
      thing -> {:ok, thing}
    end
  end

  @gql """
  query GetSomething($id: UUID!) {
    thing(id: $id) {
      id
      name
    }
  }
  """
  def get_thing(client, thing_id) do
    case Client.query(client, @gql, id: thing_id) do
      {:ok, %{"data" => %{"thing" => nil}}} ->
        nil

      {:ok, %{"data" => %{"thing" => result}}} ->
        {:ok, result}

      {:ok, errors} when is_list(errors) ->
        nil
    end
  end

  @gql """
  subscription ThingChanges($thingId: String!){
    thingChanges(thingId: $projectId) {
      id
      name
    }
  }
  """
  # handler is a pid for a process that implements `handle_info/4` as below
  def thing_changes(client, thing_id: thing_id, handler: handler) do
    Client.subscribe(client, @gql, %{thingId: thing_id}, handler)
  end
end
```

An example of handle_info

```elixir
  @impl true
  def handle_info({:subscription, _id, %{"data" => %{"thingChanges" => thing_changes}}}, %{assigns: %{thing: thing}} = socket) do
    changes = thing_changes |> Enum.find(&(&1["id"] == thing.id))
    socket |> do_cool_update(changes["things"]) |> noreply()
  end
```

## Telemetry

All events are emitted via `:telemetry` from `Absinthe.GraphqlWS.Transport`. Attach handlers with
`:telemetry.attach/4` or `:telemetry.attach_many/4`.

**Convention:** numeric load signals belong in **measurements**; dimensions belong in **metadata**.
Several lifecycle events include `socket_subscription_count` in measurements so consumers can build
per-socket subscription load distributions without high-cardinality tags.

### Shared metadata

Most events include some or all of these socket fields (from `socket.assigns`):

- `platform` — `socket.assigns[:platform]` (defaults to `"unknown"`)
- `session_id` — `socket.assigns[:session_id]`
- `client_app_version` — `socket.assigns[:client_app_version]`
- `user_id` — `socket.assigns[:user_id]`

Operation-scoped events also include `id` (GraphQL-WS message id) and `operation_name`.

### Transport lifecycle

- `[:absinthe_graphql_ws, :transport, :init]` — WebSocket upgrade (`Transport.init/1`). Measurements:
  none. Metadata: shared socket metadata.
- `[:absinthe_graphql_ws, :transport, :terminate]` — Socket process shutdown
  (`Transport.terminate/2`). Measurements: `socket_subscription_count`. Metadata: shared socket
  metadata, `reason`, `subscriptions` (list of `%{id, operation_name}`; `[]` on clean close).

### `handle_inbound` (client JSON frames)

- `[:absinthe_graphql_ws, :handle_inbound, :connection_init]` — Successful `connection_init`.
  Measurements: `socket_subscription_count` (`0`). Metadata: shared socket metadata, `auth_status`.
- `[:absinthe_graphql_ws, :handle_inbound, :subscribe]` — Subscription registered (or query/mutation
  `next` reply). Measurements: `socket_subscription_count`. Metadata: operation metadata.
- `[:absinthe_graphql_ws, :handle_inbound, :complete, :start]` — Client `complete` frame; start of
  unsubscribe. Measurements: `monotonic_time`, `system_time`. Metadata: operation metadata,
  `memory_before`, `message_queue_len_before`.
- `[:absinthe_graphql_ws, :handle_inbound, :complete, :stop]` — Client `complete` frame; end of
  unsubscribe (lifecycle counter attaches here). Measurements: `duration`, `monotonic_time`,
  `socket_subscription_count`. Metadata: span start metadata plus `memory_after`, `memory_delta`,
  `message_queue_len_after`.
- `[:absinthe_graphql_ws, :handle_inbound, :ping]` — Client `ping` frame. Measurements:
  `payload_size`, `socket_subscription_count`. Metadata: shared socket metadata.
- `[:absinthe_graphql_ws, :handle_inbound, :error]` — Protocol error before close (e.g. duplicate
  init, subscribe before init). Measurements: none. Metadata: shared socket metadata, `code`,
  `operation`, `reason`.

### `handle_info` (internal messages)

- `[:absinthe_graphql_ws, :handle_info, :broadcast]` — Subscription publish to client. Measurements:
  `payload_size`. Metadata: shared socket metadata, `payload`, `operation_name`.

### Keepalive (server-side control frames)

- `[:absinthe_graphql_ws, :keepalive, :start]` — Outbound `:ping` scheduled. Measurements:
  `system_time`. Metadata: none.
- `[:absinthe_graphql_ws, :keepalive, :stop]` — Inbound `:pong` received. Measurements: `duration`.
  Metadata: none.

### Garbage collection (optional, when `gc_interval` assign is set)

- `[:absinthe_graphql_ws, :gc, :start]` — GC span start. Measurements: `monotonic_time`,
  `system_time`. Metadata: `before` (process info map).
- `[:absinthe_graphql_ws, :gc, :stop]` — GC span stop. Measurements: `duration`. Metadata: `before`,
  `after` (process info maps).

### Example

```elixir
:telemetry.attach(
  "absinthe-graphql-ws-transport-init",
  [:absinthe_graphql_ws, :transport, :init],
  fn _event, measurements, metadata, _config ->
    # increment counter, tag with metadata.platform, etc.
  end,
  nil
)
```

## Benchmarks

Benchmarks live in the `benchmarks` directory, and can be run with `MIX_ENV=bench mix run
benchmarks/<file>`.

## Contributing

- Pull requests that may be rebased are preferrable to merges or squashes.
- Please **do not** increment the version number in pull requests.
