%%%
%%%   Copyright (c) 2014-2021, Klarna Bank AB (publ)
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

-module(brod).
-behaviour(application).

%% Application
-export([ start/0
        , start/2
        , stop/0
        , stop/1
        ]).

%% Client API
-export([ get_partitions_count/2
        , start_client/1
        , start_client/2
        , start_client/3
        , start_consumer/3
        , start_producer/3
        , stop_client/1
        ]).

-export([ start_link_client/1
        , start_link_client/2
        , start_link_client/3
        ]).

%% Producer API
-export([ get_producer/3
        , produce/2
        , produce/3
        , produce/5
        , produce_cb/4
        , produce_cb/6
        , produce_sync/2
        , produce_sync/3
        , produce_sync/5
        , produce_sync_offset/5
        , produce_no_ack/5
        , sync_produce_request/1
        , sync_produce_request/2
        , sync_produce_request_offset/1
        , sync_produce_request_offset/2
        ]).

%% Simple Consumer API
-export([ consume_ack/2
        , consume_ack/4
        , get_consumer/3
        , subscribe/3
        , subscribe/5
        , unsubscribe/1
        , unsubscribe/2
        , unsubscribe/3
        , unsubscribe/4
        ]).

%% Subscriber API
-export([ start_link_group_subscriber_v2/1
        , start_link_topic_subscriber/1
        ]).

%% Deprecated API
-export([ start_link_group_subscriber/7
        , start_link_group_subscriber/8
        , start_link_topic_subscriber/5
        , start_link_topic_subscriber/6
        , start_link_topic_subscriber/7
        ]).

%% Topic APIs
-export([ create_topics/3
        , create_topics/4
        , delete_topics/3
        , delete_topics/4
        ]).

%% APIs for quick metadata or message inspection and brod_cli
-export([ get_metadata/1
        , get_metadata/2
        , get_metadata/3
        , resolve_offset/3
        , resolve_offset/4
        , resolve_offset/5
        , resolve_offset/6
        , fetch/4
        , fetch/5
        , fold/8
        , connect_leader/4
        , list_all_groups/2
        , list_groups/2
        , describe_groups/3
        , connect_group_coordinator/3
        , fetch_committed_offsets/2
        , fetch_committed_offsets/3
        ]).

-deprecated([ {fetch, 7, next_version}
            , {fetch, 8, next_version}
            ]).

-export([ fetch/7
        , fetch/8
        ]).

-ifdef(build_brod_cli).
-export([main/1]).
-endif.

-export_type([ batch_input/0
             , bootstrap/0
             , call_ref/0
             , cg/0
             , cg_protocol_type/0
             , client/0
             , client_config/0
             , client_id/0
             , compression/0
             , connection/0
             , conn_config/0
             , consumer_config/0
             , consumer_option/0
             , consumer_options/0
             , endpoint/0
             , error_code/0
             , fetch_opts/0
             , fold_acc/0
             , fold_fun/1
             , fold_limits/0
             , fold_stop_reason/0
             , fold_result/0
             , group_config/0
             , group_generation_id/0
             , group_id/0
             , group_member/0
             , group_member_id/0
             , hostname/0
             , key/0
             , msg_input/0
             , msg_ts/0
             , message/0
             , message_set/0
             , offset/0
             , offset_time/0
             , partition/0
             , partition_assignment/0
             , partition_fun/0
             , partitioner/0
             , portnum/0
             , produce_ack_cb/0
             , producer_config/0
             , produce_reply/0
             , produce_result/0
             , received_assignments/0
             , topic/0
             , topic_partition/0
             , value/0
             ]).

-include("brod_int.hrl").

%%%_* Types ====================================================================

%% basics
-type hostname() :: kpro:hostname().
-type portnum() :: pos_integer().
-type endpoint() :: {hostname(), portnum()}.
-type topic() :: kpro:topic().
-type topic_config() :: kpro:struct().
-type partition() :: kpro:partition().
-type topic_partition() :: {topic(), partition()}.
-type offset() :: kpro:offset().
-type key() :: undefined %% no key, transformed to <<>>
             | binary().
-type value() :: undefined %% no value, transformed to <<>>
               | iodata() %% single value
               | {msg_ts(), binary()} %% one message with timestamp
               | [?KV(key(), value())] %% backward compatible
               | [?TKV(msg_ts(), key(), value())] %% backward compatible
               | kpro:msg_input() %% one magic v2 message
               | kpro:batch_input(). %% maybe nested batch

-type msg_input() :: kpro:msg_input().
-type batch_input() :: [msg_input()].

-type msg_ts() :: kpro:msg_ts().
-type client_id() :: atom().
-type client() :: client_id() | pid().
-type client_config() :: brod_client:config().
-type bootstrap() :: [endpoint()] %% default client config
                   | {[endpoint()], client_config()}.
-type offset_time() :: integer()
                     | ?OFFSET_EARLIEST
                     | ?OFFSET_LATEST.
-type message() :: kpro:message().
-type message_set() :: #kafka_message_set{}.
-type error_code() :: kpro:error_code().

%% producers
-type produce_reply() :: #brod_produce_reply{}.
-type producer_config() :: brod_producer:config().
-type partition_fun() :: fun((topic(), pos_integer(), key(), value()) ->
                                {ok, partition()}).
-type partitioner() :: partition_fun() | random | hash.
-type produce_ack_cb() :: fun((partition(), offset()) -> _).
-type compression() :: no_compression | gzip | snappy.
-type call_ref() :: #brod_call_ref{}.
-type produce_result() :: brod_produce_req_buffered
                        | brod_produce_req_acked.


%% consumers
-type consumer_option() :: begin_offset
                         | min_bytes
                         | max_bytes
                         | max_wait_time
                         | sleep_timeout
                         | prefetch_count
                         | prefetch_bytes
                         | offset_reset_policy
                         | size_stat_window.
-type consumer_options() :: [{consumer_option(), integer()}].
-type consumer_config() :: brod_consumer:config().
-type connection() :: kpro:connection().
-type conn_config() :: [{atom(), term()}] | kpro:conn_config().

%% consumer groups
-type group_id() :: kpro:group_id().
-type group_member_id() :: binary().
-type group_member() :: {group_member_id(), #kafka_group_member_metadata{}}.
-type group_generation_id() :: non_neg_integer().
-type group_config() :: proplists:proplist().
-type partition_assignment() :: {topic() , [partition()]}.
-type received_assignments() :: [#brod_received_assignment{}].
-type cg() :: #brod_cg{}.
-type cg_protocol_type() :: binary().
-type fetch_opts() :: kpro:fetch_opts().
-type fold_acc() :: term().
-type fold_fun(Acc) :: fun((message(), Acc) -> {ok, Acc} | {error, any()}).
%% `fold' always returns when reaches the high watermark offset. `fold'
%% also returns when any of the limits is hit.
-type fold_limits() :: #{ message_count => pos_integer()
                        , reach_offset => offset()
                        }.
-type fold_stop_reason() :: reached_end_of_partition
                          | reached_message_count_limit
                          | reached_target_offset
                          | {error, any()}.
%% OffsetToContinue: begin offset for the next fold call
-type fold_result() :: ?BROD_FOLD_RET(fold_acc(),
                                      OffsetToContinue :: offset(),
                                      fold_stop_reason()).

%%%_* APIs =====================================================================

%% @doc Start brod application.
-spec start() -> ok | no_return().
start() ->
  {ok, _Apps} = application:ensure_all_started(brod),
  ok.

%% @doc Stop brod application.
-spec stop() -> ok.
stop() ->
  application:stop(brod).

%% @doc Application behaviour callback
start(_StartType, _StartArgs) -> brod_sup:start_link().

%% @doc Application behaviour callback
stop(_State) -> ok.

%% @equiv start_client(BootstrapEndpoints, brod_default_client)
-spec start_client([endpoint()]) -> ok | {error, any()}.
start_client(BootstrapEndpoints) ->
  start_client(BootstrapEndpoints, ?BROD_DEFAULT_CLIENT_ID).

%% @equiv start_client(BootstrapEndpoints, ClientId, [])
-spec start_client([endpoint()], client_id()) -> ok | {error, any()}.
start_client(BootstrapEndpoints, ClientId) ->
  start_client(BootstrapEndpoints, ClientId, []).

%% @doc Start a client.
%%
%% `BootstrapEndpoints':
%%   Kafka cluster endpoints, can be any of the brokers in the cluster,
%%   which does not necessarily have to be the leader of any partition,
%%   e.g. a load-balanced entrypoint to the remote Kafka cluster.
%%
%% `ClientId': Atom to identify the client process.
%%
%% `Config' is a proplist, possible values:
%%  <ul>
%%   <li>`restart_delay_seconds' (optional, default=10)
%%
%%     How long to wait between attempts to restart brod_client
%%     process when it crashes.</li>
%%
%%   <li>`get_metadata_timeout_seconds' (optional, default=5)
%%
%%     Return `{error, timeout}' from `brod_client:get_xxx' calls if
%%     responses for APIs such as `metadata', `find_coordinator'
%%     are not received in time.</li>
%%
%%   <li>`reconnect_cool_down_seconds' (optional, default=1)
%%
%%     Delay this configured number of seconds before retrying to
%%     estabilish a new connection to the kafka partition leader.</li>
%%
%%   <li>`allow_topic_auto_creation' (optional, default=true)
%%
%%     By default, brod respects what is configured in the broker
%%     about topic auto-creation. i.e. whether
%%     `auto.create.topics.enable' is set in the broker configuration.
%%     However if `allow_topic_auto_creation' is set to `false' in
%%     client config, brod will avoid sending metadata requests that
%%     may cause an auto-creation of the topic regardless of what
%%     broker config is.</li>
%%
%%   <li>`auto_start_producers' (optional, default=false)
%%
%%     If true, brod client will spawn a producer automatically when
%%     user is trying to call `produce' but did not call `brod:start_producer'
%%     explicitly. Can be useful for applications which don't know beforehand
%%     which topics they will be working with.</li>
%%
%%   <li>`default_producer_config' (optional, default=[])
%%
%%     Producer configuration to use when auto_start_producers is true.
%%     See `brod_producer:start_link/4' for details about producer config</li>
%%
%% </ul>
%%
%% Connection options can be added to the same proplist. See
%% `kpro_connection.erl' in `kafka_protocol' for the details:
%%
%% <ul>
%%   <li>`ssl' (optional, default=false)
%%
%%     `true | false | ssl:ssl_option()'
%%     `true' is translated to `[]' as `ssl:ssl_option()' i.e. all default.
%%   </li>
%%
%%   <li>`sasl' (optional, default=undefined)
%%
%%     Credentials for SASL/Plain authentication.
%%     `{mechanism(), Filename}' or `{mechanism(), UserName, Password}'
%%     where mechanism can be atoms: `plain' (for "PLAIN"), `scram_sha_256'
%%     (for "SCRAM-SHA-256") or `scram_sha_512' (for SCRAM-SHA-512).
%%     `Filename' should be a file consisting two lines, first line
%%     is the username and the second line is the password.
%%     Both `Username' and `Password' should be `string() | binary()'</li>
%%
%%   <li>`connect_timeout' (optional, default=5000)
%%
%%     Timeout when trying to connect to an endpoint.</li>
%%
%%   <li>`request_timeout' (optional, default=240000, constraint: >= 1000)
%%
%%     Timeout when waiting for a response, connection restart when timed
%%     out.</li>
%%
%%   <li>`query_api_versions' (optional, default=true)
%%
%%     Must be set to false to work with kafka versions prior to 0.10,
%%     When set to `true', at connection start, brod will send a query request
%%     to get the broker supported API version ranges.
%%     When set to 'false', brod will alway use the lowest supported API version
%%     when sending requests to kafka.
%%     Supported API version ranges can be found in:
%%     `brod_kafka_apis:supported_versions/1'</li>
%%
%%   <li>`extra_sock_opts' (optional, default=[])
%%
%%     Extra socket options to tune socket performance.
%%     e.g. `[{sndbuf, 1 bsl 20}]'.
%%     <a href="http://erlang.org/doc/man/gen_tcp.html#type-option">More info
%%     </a>
%%   </li>
%% </ul>
-spec start_client([endpoint()], client_id(), client_config()) ->
                      ok | {error, any()}.
start_client(BootstrapEndpoints, ClientId, Config) ->
  case brod_sup:start_client(BootstrapEndpoints, ClientId, Config) of
    ok                               -> ok;
    {error, {already_started, _Pid}} -> ok;
    {error, Reason}                  -> {error, Reason}
  end.

%% @equiv start_link_client(BootstrapEndpoints, brod_default_client)
-spec start_link_client([endpoint()]) -> {ok, pid()} | {error, any()}.
start_link_client(BootstrapEndpoints) ->
  start_link_client(BootstrapEndpoints, ?BROD_DEFAULT_CLIENT_ID).

%% @equiv start_link_client(BootstrapEndpoints, ClientId, [])
-spec start_link_client([endpoint()], client_id()) ->
        {ok, pid()} | {error, any()}.
start_link_client(BootstrapEndpoints, ClientId) ->
  start_link_client(BootstrapEndpoints, ClientId, []).

-spec start_link_client([endpoint()], client_id(), client_config()) ->
        {ok, pid()} | {error, any()}.
start_link_client(BootstrapEndpoints, ClientId, Config) ->
  brod_client:start_link(BootstrapEndpoints, ClientId, Config).

%% @doc Stop a client.
-spec stop_client(client()) -> ok.
stop_client(Client) when is_atom(Client) ->
  case brod_sup:find_client(Client) of
    [_Pid] -> brod_sup:stop_client(Client);
    []     -> brod_client:stop(Client)
  end;
stop_client(Client) when is_pid(Client) ->
  brod_client:stop(Client).

%% @doc Dynamically start a per-topic producer.
%% @see brod_producer:start_link/4
-spec start_producer(client(), topic(), producer_config()) ->
                        ok | {error, any()}.
start_producer(Client, TopicName, ProducerConfig) ->
  brod_client:start_producer(Client, TopicName, ProducerConfig).

%% @doc Dynamically start a topic consumer.
%% @see brod_consumer:start_link/5. for details about consumer config.
-spec start_consumer(client(), topic(), consumer_config()) ->
                        ok | {error, any()}.
start_consumer(Client, TopicName, ConsumerConfig) ->
  brod_client:start_consumer(Client, TopicName, ConsumerConfig).

%% @doc Get number of partitions for a given topic.
%% The higher level producers may need the partition numbers to
%% find the partition producer pid --- if the number of partitions
%% is not statically configured for them.
%% It is up to the callers how they want to distribute their data
%% (e.g. random, roundrobin or consistent-hashing) to the partitions.
-spec get_partitions_count(client(), topic()) ->
        {ok, pos_integer()} | {error, any()}.
get_partitions_count(Client, Topic) ->
  brod_client:get_partitions_count(Client, Topic).

-spec get_consumer(client(), topic(), partition()) ->
        {ok, pid()} | {error, Reason}
          when Reason :: client_down
                       | {consumer_down, noproc}
                       | {consumer_not_found, topic()}
                       | {consumer_not_found, topic(), partition()}.
get_consumer(Client, Topic, Partition) ->
  brod_client:get_consumer(Client, Topic, Partition).

%% @equiv brod_client:get_producer/3
-spec get_producer(client(), topic(), partition()) ->
        {ok, pid()} | {error, Reason}
          when Reason :: client_down
                       | {producer_down, noproc}
                       | {producer_not_found, topic()}
                       | {producer_not_found, topic(), partition()}.
get_producer(Client, Topic, Partition) ->
  brod_client:get_producer(Client, Topic, Partition).

%% @equiv produce(Pid, 0, <<>>, Value)
-spec produce(pid(), value()) -> {ok, call_ref()} | {error, any()}.
produce(Pid, Value) ->
  produce(Pid, _Key = <<>>, Value).

%% @doc Produce one message if `Value' is a binary or an
%% iolist. Otherwise send a batch, if `Value' is a (nested) key-value
%% list, or a list of maps. In this case `Key' is discarded (only the
%% keys in the key-value list are sent to Kafka). The pid should be a
%% partition producer pid, NOT client pid.  The return value is a call
%% reference of type `call_ref()', so the caller can use it to expect
%% (match) a `#brod_produce_reply{result = brod_produce_req_acked}'
%% message after the produce request has been acked by Kafka.
-spec produce(pid(), key(), value()) ->
        {ok, call_ref()} | {error, any()}.
produce(ProducerPid, Key, Value) ->
  brod_producer:produce(ProducerPid, Key, Value).

%% @doc Produce one message if `Value' is a binary or an iolist.
%% Otherwise send a batch if `Value' is a (nested) key-value list, or
%% a list of maps. In this case `Key' is used only for partitioning,
%% or discarded if the 3rd argument is a partition number instead of a
%% partitioner callback. This function first looks up the producer
%% pid, then calls `produce/3' to do the real work. The return value
%% is a call reference of type `call_ref()', so the caller can used it
%% to expect (match) a `#brod_produce_reply{result =
%% brod_produce_req_acked}' message after the produce request has been
%% acked by Kafka.
-spec produce(client(), topic(), partition() | partitioner(),
              key(), value()) -> {ok, call_ref()} | {error, any()}.
produce(Client, Topic, Partition, Key, Value) when is_integer(Partition) ->
  case get_producer(Client, Topic, Partition) of
    {ok, Pid} -> produce(Pid, Key, Value);
    {error, Reason} -> {error, Reason}
  end;
produce(Client, Topic, Partitioner, Key, Value) ->
  PartFun = brod_utils:make_part_fun(Partitioner),
  case brod_client:get_partitions_count(Client, Topic) of
    {ok, PartitionsCnt} ->
      {ok, Partition} = PartFun(Topic, PartitionsCnt, Key, Value),
      produce(Client, Topic, Partition, Key, Value);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Same as `produce/3', only the ack is not delivered as a message,
%% instead, the callback is evaluated by producer worker when ack is received
%% from kafka.
-spec produce_cb(pid(), key(), value(), produce_ack_cb()) ->
        ok | {error, any()}.
produce_cb(ProducerPid, Key, Value, AckCb) ->
  brod_producer:produce_cb(ProducerPid, Key, Value, AckCb).

%% @doc Same as `produce/5' only the ack is not delivered as a message,
%% instead, the callback is evaluated by producer worker when ack is received
%% from kafka. Return the partition to caller as `{ok, Partition}' for caller
%% to correlate the callback when the 3rd arg is not a partition number.
-spec produce_cb(client(), topic(), partition() | partitioner(),
                 key(), value(), produce_ack_cb()) ->
        ok | {ok, partition()} | {error, any()}.
produce_cb(Client, Topic, Part, Key, Value, AckCb) when is_integer(Part) ->
  case get_producer(Client, Topic, Part) of
    {ok, Pid} -> produce_cb(Pid, Key, Value, AckCb);
    {error, Reason} -> {error, Reason}
  end;
produce_cb(Client, Topic, Partitioner, Key, Value, AckCb) ->
  PartFun = brod_utils:make_part_fun(Partitioner),
  case brod_client:get_partitions_count(Client, Topic) of
    {ok, PartitionsCnt} ->
      {ok, Partition} = PartFun(Topic, PartitionsCnt, Key, Value),
      case produce_cb(Client, Topic, Partition, Key, Value, AckCb) of
        ok -> {ok, Partition};
        {error, Reason} -> {error, Reason}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Send the message to partition worker without any ack.
%% NOTE: This call has no back-pressure to the caller,
%%       excessive usage may cause BEAM to run out of memory.
-spec produce_no_ack(pid(), key(), value()) -> ok | {error, any()}.
produce_no_ack(ProducerPid, Key, Value) ->
  brod_producer:produce_no_ack(ProducerPid, Key, Value).

%% @doc Find the partition worker and send message without any ack.
%% NOTE: This call has no back-pressure to the caller,
%%       excessive usage may cause BEAM to run out of memory.
-spec produce_no_ack(client(), topic(), partition() | partitioner(),
           key(), value()) -> ok | {error, any()}.
produce_no_ack(Client, Topic, Part, Key, Value) when is_integer(Part) ->
  case get_producer(Client, Topic, Part) of
    {ok, Pid} -> produce_no_ack(Pid, Key, Value);
    {error, Reason} -> {error, Reason}
  end;
produce_no_ack(Client, Topic, Partitioner, Key, Value) ->
  PartFun = brod_utils:make_part_fun(Partitioner),
  case brod_client:get_partitions_count(Client, Topic) of
    {ok, PartitionsCnt} ->
      {ok, Partition} = PartFun(Topic, PartitionsCnt, Key, Value),
      produce_no_ack(Client, Topic, Partition, Key, Value);
    {error, _Reason} ->
      %% error ignored
      ok
  end.

%% @doc Same as `produce/5' only the ack is not d
%% @equiv produce_sync(Pid, 0, <<>>, Value)
-spec produce_sync(pid(), value()) -> ok.
produce_sync(Pid, Value) ->
  produce_sync(Pid, _Key = <<>>, Value).

%% @doc Sync version of produce/3
%%
%% This function will not return until the response is received from
%% Kafka. But when producer is started with `required_acks' set to 0,
%% this function will return once the messages are buffered in the
%% producer process.
-spec produce_sync(pid(), key(), value()) ->
        ok | {error, any()}.
produce_sync(Pid, Key, Value) ->
  case produce(Pid, Key, Value) of
    {ok, CallRef} ->
      %% Wait until the request is acked by kafka
      sync_produce_request(CallRef);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Sync version of produce/5
%% This function will not return until a response is received from kafka,
%% however if producer is started with required_acks set to 0, this function
%% will return once the messages are buffered in the producer process.
-spec produce_sync(client(), topic(), partition() | partitioner(),
                   key(), value()) -> ok | {error, any()}.
produce_sync(Client, Topic, Partition, Key, Value) ->
  case produce_sync_offset(Client, Topic, Partition, Key, Value) of
    {ok, _} -> ok;
    Else -> Else
  end.

%% @doc Version of produce_sync/5 that returns the offset assigned by Kafka
%% If producer is started with required_acks set to 0, the offset will be
%% `?BROD_PRODUCE_UNKNOWN_OFFSET'.
-spec produce_sync_offset(client(), topic(), partition() | partitioner(),
                          key(), value()) -> {ok, offset()} | {error, any()}.
produce_sync_offset(Client, Topic, Partition, Key, Value) ->
  case produce(Client, Topic, Partition, Key, Value) of
    {ok, CallRef} ->
      sync_produce_request_offset(CallRef);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Block wait for sent produced request to be acked by kafka.
-spec sync_produce_request(call_ref()) ->
        ok | {error, Reason :: any()}.
sync_produce_request(CallRef) ->
  sync_produce_request(CallRef, infinity).

-spec sync_produce_request(call_ref(), timeout()) ->
        ok | {error, Reason :: any()}.
sync_produce_request(CallRef, Timeout) ->
  case sync_produce_request_offset(CallRef, Timeout) of
    {ok, _} -> ok;
    Else -> Else
  end.

%% @doc As sync_produce_request_offset/1, but also returning assigned offset
%% See produce_sync_offset/5.
-spec sync_produce_request_offset(call_ref()) ->
        {ok, offset()} | {error, Reason :: any()}.
sync_produce_request_offset(CallRef) ->
  sync_produce_request_offset(CallRef, infinity).

-spec sync_produce_request_offset(call_ref(), timeout()) ->
        {ok, offset()} | {error, Reason :: any()}.
sync_produce_request_offset(CallRef, Timeout) ->
  brod_producer:sync_produce_request(CallRef, Timeout).

%% @doc Subscribe to a data stream from the given topic-partition.
%%
%% If `{error, Reason}' is returned, the caller should perhaps retry later.
%%
%% `{ok, ConsumerPid}' is returned on success. The caller may want to
%% monitor the consumer pid and re-subscribe should the `ConsumerPid' crash.
%%
%% Upon successful subscription the subscriber process should expect messages
%% of pattern:
%% `{ConsumerPid, #kafka_message_set{}}' and
%% `{ConsumerPid, #kafka_fetch_error{}}'.
%%
%% `-include_lib("brod/include/brod.hrl")' to access the records.
%%
%% In case `#kafka_fetch_error{}' is received the subscriber should
%% re-subscribe itself to resume the data stream.
-spec subscribe(client(), pid(), topic(), partition(),
                consumer_options()) -> {ok, pid()} | {error, any()}.
subscribe(Client, SubscriberPid, Topic, Partition, Options) ->
  case brod_client:get_consumer(Client, Topic, Partition) of
    {ok, ConsumerPid} ->
      case subscribe(ConsumerPid, SubscriberPid, Options) of
        ok    -> {ok, ConsumerPid};
        Error -> Error
      end;
    {error, Reason} ->
      {error, Reason}
  end.

-spec subscribe(pid(), pid(), consumer_options()) -> ok | {error, any()}.
subscribe(ConsumerPid, SubscriberPid, Options) ->
  brod_consumer:subscribe(ConsumerPid, SubscriberPid, Options).

%% @doc Unsubscribe the current subscriber. Assuming the subscriber is
%% `self()'.
-spec unsubscribe(client(), topic(), partition()) -> ok | {error, any()}.
unsubscribe(Client, Topic, Partition) ->
  unsubscribe(Client, Topic, Partition, self()).

%% @doc Unsubscribe the current subscriber.
-spec unsubscribe(client(), topic(), partition(), pid()) -> ok | {error, any()}.
unsubscribe(Client, Topic, Partition, SubscriberPid) ->
  case brod_client:get_consumer(Client, Topic, Partition) of
    {ok, ConsumerPid} -> unsubscribe(ConsumerPid, SubscriberPid);
    Error             -> Error
  end.

%% @doc Unsubscribe the current subscriber. Assuming the subscriber is
%% `self()'.
-spec unsubscribe(pid()) -> ok | {error, any()}.
unsubscribe(ConsumerPid) ->
  unsubscribe(ConsumerPid, self()).

%% @doc Unsubscribe the current subscriber.
-spec unsubscribe(pid(), pid()) -> ok | {error, any()}.
unsubscribe(ConsumerPid, SubscriberPid) ->
  brod_consumer:unsubscribe(ConsumerPid, SubscriberPid).

-spec consume_ack(client(), topic(), partition(), offset()) ->
        ok | {error, any()}.
consume_ack(Client, Topic, Partition, Offset) ->
  case brod_client:get_consumer(Client, Topic, Partition) of
    {ok, ConsumerPid} -> consume_ack(ConsumerPid, Offset);
    {error, Reason}   -> {error, Reason}
  end.

-spec consume_ack(pid(), offset()) -> ok | {error, any()}.
consume_ack(ConsumerPid, Offset) ->
  brod_consumer:ack(ConsumerPid, Offset).

%% @see brod_group_subscriber:start_link/7
-spec start_link_group_subscriber(
        client(), group_id(), [topic()],
        group_config(), consumer_config(), module(), term()) ->
          {ok, pid()} | {error, any()}.
start_link_group_subscriber(Client, GroupId, Topics, GroupConfig,
                            ConsumerConfig, CbModule, CbInitArg) ->
  brod_group_subscriber:start_link(Client, GroupId, Topics, GroupConfig,
                                   ConsumerConfig, CbModule, CbInitArg).

%% @doc Start group_subscriber_v2
-spec start_link_group_subscriber_v2(
        brod_group_subscriber_v2:subscriber_config()
       ) -> {ok, pid()} | {error, any()}.
start_link_group_subscriber_v2(Config) ->
  brod_group_subscriber_v2:start_link(Config).

%% @see brod_group_subscriber:start_link/8
-spec start_link_group_subscriber(
        client(), group_id(), [topic()], group_config(),
        consumer_config(), message | message_set,
        module(), term()) ->
          {ok, pid()} | {error, any()}.
start_link_group_subscriber(Client, GroupId, Topics, GroupConfig,
                            ConsumerConfig, MessageType,
                            CbModule, CbInitArg) ->
  brod_group_subscriber:start_link(Client, GroupId, Topics, GroupConfig,
                                   ConsumerConfig, MessageType,
                                   CbModule, CbInitArg).

%% @equiv start_link_topic_subscriber(Client, Topic, 'all', ConsumerConfig,
%%                                    CbModule, CbInitArg)
%% @deprecated Please use {@link start_link_topic_subscriber/1} instead
-spec start_link_topic_subscriber(
        client(), topic(), consumer_config(), module(), term()) ->
          {ok, pid()} | {error, any()}.
start_link_topic_subscriber(Client, Topic, ConsumerConfig,
                            CbModule, CbInitArg) ->
  start_link_topic_subscriber(Client, Topic, all, ConsumerConfig,
                              CbModule, CbInitArg).

%% @equiv start_link_topic_subscriber(Client, Topic, Partitions,
%%                                    ConsumerConfig, message,
%%                                    CbModule, CbInitArg)
%% @deprecated Please use {@link start_link_topic_subscriber/1} instead
-spec start_link_topic_subscriber(
        client(), topic(), all | [partition()],
        consumer_config(), module(), term()) ->
          {ok, pid()} | {error, any()}.
start_link_topic_subscriber(Client, Topic, Partitions,
                            ConsumerConfig, CbModule, CbInitArg) ->
  start_link_topic_subscriber(Client, Topic, Partitions,
                              ConsumerConfig, message, CbModule, CbInitArg).

%% @see brod_topic_subscriber:start_link/7
%% @deprecated Please use {@link start_link_topic_subscriber/1} instead
-spec start_link_topic_subscriber(
        client(), topic(), all | [partition()],
        consumer_config(), message | message_set,
        module(), term()) ->
          {ok, pid()} | {error, any()}.
start_link_topic_subscriber(Client, Topic, Partitions,
                            ConsumerConfig, MessageType, CbModule, CbInitArg) ->
  brod_topic_subscriber:start_link(Client, Topic, Partitions,
                                   ConsumerConfig, MessageType,
                                   CbModule, CbInitArg).

%% @see brod_topic_subscriber:start_link/1
-spec start_link_topic_subscriber(
        brod_topic_subscriber:topic_subscriber_config()
       ) -> {ok, pid()} | {error, any()}.
start_link_topic_subscriber(Config) ->
  brod_topic_subscriber:start_link(Config).

%% @equiv create_topics(Hosts, TopicsConfigs, RequestConfigs, [])
-spec create_topics([endpoint()], [topic_config()], #{timeout => kpro:int32(),
                    validate_only => boolean()}) ->
        ok | {ok, kpro:struct()} | {error, any()}.
create_topics(Hosts, TopicConfigs, RequestConfigs) ->
  brod_utils:create_topics(Hosts, TopicConfigs, RequestConfigs).

%% @doc Create topic(s) in kafka
%% Return the message body of `create_topics', response.
%% See `kpro_schema.erl' for struct details
-spec create_topics([endpoint()], [topic_config()], #{timeout => kpro:int32(),
                    validate_only => boolean()}, conn_config()) ->
        ok | {ok, kpro:struct()} | {error, any()}.
create_topics(Hosts, TopicConfigs, RequestConfigs, Options) ->
  brod_utils:create_topics(Hosts, TopicConfigs, RequestConfigs, Options).

%% @equiv delete_topics(Hosts, Topics, Timeout, [])
-spec delete_topics([endpoint()], [topic()], pos_integer()) ->
        ok | {ok, kpro:struct()} | {error, any()}.
delete_topics(Hosts, Topics, Timeout) ->
  brod_utils:delete_topics(Hosts, Topics, Timeout).

%% @doc Delete topic(s) from kafka
%% Return the message body of `delete_topics', response.
%% See `kpro_schema.erl' for struct details
-spec delete_topics([endpoint()], [topic()], pos_integer(), conn_config()) ->
        ok | {ok, kpro:struct()} | {error, any()}.
delete_topics(Hosts, Topics, Timeout, Options) ->
  brod_utils:delete_topics(Hosts, Topics, Timeout, Options).

%% @doc Fetch broker metadata
%% Return the message body of `metadata' response.
%% See `kpro_schema.erl' for details
-spec get_metadata([endpoint()]) -> {ok, kpro:struct()} | {error, any()}.
get_metadata(Hosts) ->
  brod_utils:get_metadata(Hosts).

%% @doc Fetch broker/topic metadata
%% Return the message body of `metadata' response.
%% See `kpro_schema.erl' for struct details
-spec get_metadata([endpoint()], all | [topic()]) ->
        {ok, kpro:struct()} | {error, any()}.
get_metadata(Hosts, Topics) ->
  brod_utils:get_metadata(Hosts, Topics).

%% @doc Fetch broker/topic metadata
%% Return the message body of `metadata' response.
%% See `kpro_schema.erl' for struct details
-spec get_metadata([endpoint()], all | [topic()], conn_config()) ->
        {ok, kpro:struct()} | {error, any()}.
get_metadata(Hosts, Topics, Options) ->
  brod_utils:get_metadata(Hosts, Topics, Options).

%% @equiv resolve_offset(Hosts, Topic, Partition, latest, 1)
-spec resolve_offset([endpoint()], topic(), partition()) ->
        {ok, offset()} | {error, any()}.
resolve_offset(Hosts, Topic, Partition) ->
  resolve_offset(Hosts, Topic, Partition, ?OFFSET_LATEST).

%% @doc Resolve semantic offset or timestamp to real offset.
-spec resolve_offset([endpoint()], topic(), partition(), offset_time()) ->
        {ok, offset()} | {error, any()}.
resolve_offset(Hosts, Topic, Partition, Time) ->
  resolve_offset(Hosts, Topic, Partition, Time, []).

%% @doc Resolve semantic offset or timestamp to real offset.
-spec resolve_offset([endpoint()], topic(), partition(),
                     offset_time(), conn_config()) ->
        {ok, offset()} | {error, any()}.
resolve_offset(Hosts, Topic, Partition, Time, ConnCfg) ->
  brod_utils:resolve_offset(Hosts, Topic, Partition, Time, ConnCfg).

%% @doc Resolve semantic offset or timestamp to real offset.
-spec resolve_offset([endpoint()], topic(), partition(),
                     offset_time(), conn_config(),
                      #{timeout => kpro:int32()}) ->
        {ok, offset()} | {error, any()}.
resolve_offset(Hosts, Topic, Partition, Time, ConnCfg, Opts) ->
  brod_utils:resolve_offset(Hosts, Topic, Partition, Time, ConnCfg, Opts).

%% @doc Fetch a single message set from the given topic-partition.
%% The first arg can either be an already established connection to leader,
%% or `{Endpoints, ConnConfig}' so to establish a new connection before fetch.
-spec fetch(connection() | client_id() | bootstrap(),
            topic(), partition(), integer()) ->
              {ok, {HwOffset :: offset(), [message()]}} | {error, any()}.
fetch(ConnOrBootstrap, Topic, Partition, Offset) ->
  Opts = #{ max_wait_time => 1000
          , min_bytes => 1
          , max_bytes => 1 bsl 20
          },
  fetch(ConnOrBootstrap, Topic, Partition, Offset, Opts).

%% @doc Fetch a single message set from the given topic-partition.
%% The first arg can either be an already established connection to leader,
%% or `{Endpoints, ConnConfig}' so to establish a new connection before fetch.
-spec fetch(connection() | client_id() | bootstrap(),
            topic(), partition(), offset(), fetch_opts()) ->
              {ok, {HwOffset :: offset(), [message()]}} | {error, any()}.
fetch(ConnOrBootstrap, Topic, Partition, Offset, Opts) ->
  brod_utils:fetch(ConnOrBootstrap, Topic, Partition, Offset, Opts).

%% @doc Fold through messages in a partition.
%% Works like `lists:foldl/2' but with below stop conditions:
%% * Always return after reach high watermark offset
%% * Return after the given message count limit is reached
%% * Return after the given kafka offset is reached.
%% * Return if the `FoldFun' returns an `{error, Reason}' tuple.
%% NOTE: Exceptions from evaluating `FoldFun' are not caught.
-spec fold(connection() | client_id() | bootstrap(),
           topic(), partition(), offset(), fetch_opts(),
           Acc, fold_fun(Acc), fold_limits()) ->
             fold_result() when Acc :: fold_acc().
fold(Bootstrap, Topic, Partition, Offset, Opts, Acc, Fun, Limits) ->
  brod_utils:fold(Bootstrap, Topic, Partition, Offset, Opts, Acc, Fun, Limits).

%% @deprecated
%% fetch(Hosts, Topic, Partition, Offset, Wait, MinBytes, MaxBytes, [])
-spec fetch([endpoint()], topic(), partition(), offset(),
            non_neg_integer(), non_neg_integer(), pos_integer()) ->
               {ok, [message()]} | {error, any()}.
fetch(Hosts, Topic, Partition, Offset, MaxWaitTime, MinBytes, MaxBytes) ->
  fetch(Hosts, Topic, Partition, Offset, MaxWaitTime, MinBytes, MaxBytes, []).

%% @deprecated Fetch a single message set from the given topic-partition.
-spec fetch([endpoint()], topic(), partition(), offset(),
            non_neg_integer(), non_neg_integer(), pos_integer(),
            conn_config()) -> {ok, [message()]} | {error, any()}.
fetch(Hosts, Topic, Partition, Offset,
      MaxWaitTime, MinBytes, MaxBytes, ConnConfig) ->
  FetchOpts = #{ max_wait_time => MaxWaitTime
               , min_bytes => MinBytes
               , max_bytes => MaxBytes
               },
  case fetch({Hosts, ConnConfig}, Topic, Partition, Offset, FetchOpts) of
    {ok, {_HwOffset, Batch}} -> {ok, Batch}; %% backward compatible
    {error, Reason} -> {error, Reason}
  end.

%% @doc Connect partition leader.
-spec connect_leader([endpoint()], topic(), partition(),
                     conn_config()) -> {ok, pid()}.
connect_leader(Hosts, Topic, Partition, ConnConfig) ->
  KproOptions = brod_utils:kpro_connection_options(ConnConfig),
  kpro:connect_partition_leader(Hosts, ConnConfig, Topic, Partition, KproOptions).

%% @doc List ALL consumer groups in the given kafka cluster.
%% NOTE: Exception if failed to connect any of the coordinator brokers.
-spec list_all_groups([endpoint()], conn_config()) ->
        [{endpoint(), [cg()] | {error, any()}}].
list_all_groups(Endpoints, ConnCfg) ->
  brod_utils:list_all_groups(Endpoints, ConnCfg).

%% @doc List consumer groups in the given group coordinator broker.
-spec list_groups(endpoint(), conn_config()) -> {ok, [cg()]} | {error, any()}.
list_groups(CoordinatorEndpoint, ConnCfg) ->
  brod_utils:list_groups(CoordinatorEndpoint, ConnCfg).

%% @doc Describe consumer groups. The given consumer group IDs should be all
%% managed by the coordinator-broker running at the given endpoint.
%% Otherwise error codes will be returned in the result structs.
%% Return `describe_groups' response body field named `groups'.
%% See `kpro_schema.erl' for struct details
-spec describe_groups(endpoint(), conn_config(), [group_id()]) ->
        {ok, [kpro:struct()]} | {error, any()}.
describe_groups(CoordinatorEndpoint, ConnCfg, IDs) ->
  brod_utils:describe_groups(CoordinatorEndpoint, ConnCfg, IDs).

%% @doc Connect to consumer group coordinator broker.
%%
%% Done in steps: <ol>
%% <li>Connect to any of the given bootstrap ednpoints</li>
%%
%% <li>Send group_coordinator_request to resolve group coordinator
%% endpoint</li>
%%
%% <li>Connect to the resolved endpoint and return the connection
%% pid</li></ol>
-spec connect_group_coordinator([endpoint()], conn_config(), group_id()) ->
        {ok, pid()} | {error, any()}.
connect_group_coordinator(BootstrapEndpoints, ConnCfg, GroupId) ->
  KproOptions = brod_utils:kpro_connection_options(ConnCfg),
  Args = maps:merge(KproOptions, #{type => group, id => GroupId}),
  kpro:connect_coordinator(BootstrapEndpoints, ConnCfg, Args).

%% @doc Fetch committed offsets for ALL topics in the given consumer group.
%% Return the `responses' field of the `offset_fetch' response.
%% See `kpro_schema.erl' for struct details.
-spec fetch_committed_offsets([endpoint()], conn_config(), group_id()) ->
        {ok, [kpro:struct()]} | {error, any()}.
fetch_committed_offsets(BootstrapEndpoints, ConnCfg, GroupId) ->
  brod_utils:fetch_committed_offsets(BootstrapEndpoints, ConnCfg, GroupId, []).

%% @doc Same as `fetch_committed_offsets/3',
%% but works with a started `brod_client'
-spec fetch_committed_offsets(client(), group_id()) ->
        {ok, [kpro:struct()]} | {error, any()}.
fetch_committed_offsets(Client, GroupId) ->
  brod_utils:fetch_committed_offsets(Client, GroupId, []).

-ifdef(build_brod_cli).
main(X) -> brod_cli:main(X).
-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
