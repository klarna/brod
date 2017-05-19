%%%
%%%   Copyright (c) 2014-2016, Klarna AB
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

%%%=============================================================================
%%% @doc
%%% @copyright 2014-2016 Klarna AB
%%% @end
%%% ============================================================================

-define(SASL_CONTINUE,           1).
-define(SASL_OK,                 0).
-define(SASL_FAIL,              -1).
-define(SASL_NOMEM,             -2).
-define(SASL_BUFOVER,           -3).
-define(SASL_NOMECH,            -4).
-define(SASL_BADPROT,           -5).
-define(SASL_NOTDONE,           -6).
-define(SASL_BADPARAM,          -7).
-define(SASL_TRYAGAIN,          -8).
-define(SASL_BADMAC,	        -9).
-define(SASL_NOTINIT,           -12).
-define(SASL_INTERACT,          2).
-define(SASL_BADSERV,           -10).
-define(SASL_WRONGMECH,         -11).
-define(SASL_BADAUTH,           -13).
-define(SASL_NOAUTHZ,           -14).
-define(SASL_TOOWEAK,           -15).
-define(SASL_ENCRYPT,           -16).
-define(SASL_TRANS,             -17).
-define(SASL_EXPIRED,           -18).
-define(SASL_DISABLED,          -19).
-define(SASL_NOUSER,            -20).
-define(SASL_BADVERS,           -23).
-define(SASL_UNAVAIL,           -24).
-define(SASL_NOVERIFY,          -26).
-define(SASL_PWLOCK,            -21).
-define(SASL_NOCHANGE,          -22).
-define(SASL_WEAKPASS,          -27).
-define(SASL_NOUSERPASS,        -28).
-define(SASL_NEED_OLD_PASSWD,   -29).
-define(SASL_CONSTRAINT_VIOLAT,	-30).
-define(SASL_BADBINDING,        -32).

%%%_* Module declaration =======================================================
%% @private
-module(brod_sock).

%%%_* Exports ==================================================================

%% API
-export([ get_tcp_sock/1
        , init/5
        , loop/2
        , request_sync/3
        , request_async/2
        , start/4
        , start/5
        , start_link/4
        , start_link/5
        , stop/1
        , debug/2
        ]).

%% system calls support for worker process
-export([ system_continue/3
        , system_terminate/4
        , system_code_change/4
        , format_status/2
        ]).

-epxort_type([ options/0
             ]).

-define(DEFAULT_CONNECT_TIMEOUT, timer:seconds(5)).
-define(DEFAULT_REQUEST_TIMEOUT, timer:minutes(4)).
-define(SIZE_HEAD_BYTES, 4).

%%%_* Includes =================================================================
-include("brod_int.hrl").

-type opt_key() :: connect_timeout
                 | request_timeout
                 | ssl.
-type opt_val() :: term().
-type options() :: [{opt_key(), opt_val()}].
-type requests() :: brod_kafka_requests:requests().
-type byte_count() :: non_neg_integer().

-record(acc, { expected_size = error(bad_init) :: byte_count()
             , acc_size = 0 :: byte_count()
             , acc_buffer = [] :: [binary()] %% received bytes in reversed order
             }).

-type acc() :: binary() | #acc{}.

-record(state, { client_id   :: binary()
               , parent      :: pid()
               , sock        :: port()
               , acc = <<>>  :: acc()
               , requests    :: requests()
               , mod         :: gen_tcp | ssl
               , req_timeout :: timeout()
               }).

%%%_* API ======================================================================

%% @equiv start_link(Parent, Host, Port, ClientId, [])
start_link(Parent, Host, Port, ClientId) ->
  start_link(Parent, Host, Port, ClientId, []).

-spec start_link(pid(), hostname(), portnum(),
                 brod_client_id() | binary(), term()) ->
                    {ok, pid()} | {error, any()}.
start_link(Parent, Host, Port, ClientId, Options) when is_atom(ClientId) ->
  BinClientId = list_to_binary(atom_to_list(ClientId)),
  start_link(Parent, Host, Port, BinClientId, Options);
start_link(Parent, Host, Port, ClientId, Options) when is_binary(ClientId) ->
  proc_lib:start_link(?MODULE, init, [Parent, Host, Port, ClientId, Options]).

%% @equiv start(Parent, Host, Port, ClientId, [])
start(Parent, Host, Port, ClientId) ->
  start(Parent, Host, Port, ClientId, []).

-spec start(pid(), hostname(), portnum(),
            brod_client_id() | binary(), term()) ->
               {ok, pid()} | {error, any()}.
start(Parent, Host, Port, ClientId, Options) when is_atom(ClientId) ->
  BinClientId = list_to_binary(atom_to_list(ClientId)),
  start(Parent, Host, Port, BinClientId, Options);
start(Parent, Host, Port, ClientId, Options) when is_binary(ClientId) ->
  proc_lib:start(?MODULE, init, [Parent, Host, Port, ClientId, Options]).

-spec request_async(pid(), term()) -> {ok, corr_id()} | ok | {error, any()}.
request_async(Pid, Request) ->
  case call(Pid, {send, Request}) of
    {ok, CorrId} ->
      case Request of
        #kpro_ProduceRequest{requiredAcks = 0} -> ok;
        _                                      -> {ok, CorrId}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

-spec request_sync(pid(), term(), timeout()) ->
        {ok, term()} | ok | {error, any()}.
request_sync(Pid, Request, Timeout) ->
  case request_async(Pid, Request) of
    ok              -> ok;
    {ok, CorrId}    -> wait_for_resp(Pid, Request, CorrId, Timeout);
    {error, Reason} -> {error, Reason}
  end.

-spec wait_for_resp(pid(), term(), corr_id(), timeout()) ->
        {ok, term()} | {error, any()}.
wait_for_resp(Pid, _, CorrId, Timeout) ->
  Mref = erlang:monitor(process, Pid),
  receive
    {msg, Pid, CorrId, Response} ->
      erlang:demonitor(Mref, [flush]),
      {ok, Response};
    {'DOWN', Mref, _, _, Reason} ->
      {error, {sock_down, Reason}}
  after
    Timeout ->
      erlang:demonitor(Mref, [flush]),
      {error, timeout}
  end.

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) when is_pid(Pid) ->
  call(Pid, stop);
stop(_) ->
  ok.

-spec get_tcp_sock(pid()) -> {ok, port()}.
get_tcp_sock(Pid) ->
  call(Pid, get_tcp_sock).

-spec debug(pid(), print | string() | none) -> ok.
%% @doc Enable/disable debugging on the socket process.
%%      debug(Pid, pring) prints debug info on stdout
%%      debug(Pid, File) prints debug info into a File
%%      debug(Pid, none) stops debugging
%% @end
debug(Pid, none) ->
  system_call(Pid, {debug, no_debug});
debug(Pid, print) ->
  system_call(Pid, {debug, {trace, true}});
debug(Pid, File) when is_list(File) ->
  system_call(Pid, {debug, {log_to_file, File}}).

%%%_* Internal functions =======================================================

-spec init(pid(), hostname(), portnum(), brod_client_id(), [any()]) ->
        no_return().
init(Parent, Host, Port, ClientId, Options) ->
  Debug = sys:debug_options(proplists:get_value(debug, Options, [])),
  Timeout = get_connect_timeout(Options),
  SockOpts = [{active, once}, {packet, raw}, binary, {nodelay, true}],
  case gen_tcp:connect(Host, Port, SockOpts, Timeout) of
    {ok, Sock} ->
      State0 = #state{ client_id = ClientId
                     , parent    = Parent
                     },
      %% adjusting buffer size as per recommendation at
      %% http://erlang.org/doc/man/inet.html#setopts-2
      %% idea is from github.com/epgsql/epgsql
      {ok, [{recbuf, RecBufSize}, {sndbuf, SndBufSize}]} =
        inet:getopts(Sock, [recbuf, sndbuf]),
      ok = inet:setopts(Sock, [{buffer, max(RecBufSize, SndBufSize)}]),
      SslOpts = proplists:get_value(ssl, Options, false),
      Mod = get_tcp_mod(SslOpts),
      {ok, NewSock} = maybe_upgrade_to_ssl(Sock, Mod, SslOpts, Timeout),
      ok = maybe_sasl_auth(Host, NewSock, Mod, ClientId, Timeout,
                           proplists:get_value(sasl, Options)),
      State = State0#state{mod = Mod, sock = NewSock},
      proc_lib:init_ack(Parent, {ok, self()}),
      ReqTimeout = get_request_timeout(Options),
      ok = send_assert_max_req_age(self(), ReqTimeout),
      try
        Requests = brod_kafka_requests:new(),
        loop(State#state{requests = Requests, req_timeout = ReqTimeout}, Debug)
      catch error : E ->
        Stack = erlang:get_stacktrace(),
        exit({E, Stack})
      end;
    {error, Reason} ->
      %% exit instead of {error, Reason}
      %% otherwise exit reason will be 'normal'
      exit({connection_failure, Reason})
  end.

get_tcp_mod(_SslOpts = true)  -> ssl;
get_tcp_mod(_SslOpts = [_|_]) -> ssl;
get_tcp_mod(_)                -> gen_tcp.

maybe_upgrade_to_ssl(Sock, _Mod = ssl, _SslOpts = true, Timeout) ->
  ssl:connect(Sock, [], Timeout);
maybe_upgrade_to_ssl(Sock, _Mod = ssl, SslOpts = [_|_], Timeout) ->
  ssl:connect(Sock, SslOpts, Timeout);
maybe_upgrade_to_ssl(Sock, _Mod, _SslOpts, _Timeout) ->
  {ok, Sock}.

maybe_sasl_auth(_Host, _Sock, _Mod, _ClientId, _Timeout, _SaslOpts = undefined) -> ok;
maybe_sasl_auth(_Host, Sock, Mod, ClientId, Timeout,
                _SaslOpts = {_Method = plain, SaslUser, SaslPassword}) ->
  ok = setopts(Sock, Mod, [{active, false}]),
  HandshakeRequest = #kpro_SaslHandshakeRequest{mechanism="PLAIN"},
  HandshakeRequestBin = kpro:encode_request(ClientId, 0, HandshakeRequest),
  ok = Mod:send(Sock, HandshakeRequestBin),
  {ok, <<Len:32>>} = Mod:recv(Sock, 4, Timeout),
  {ok, HandshakeResponseBin} = Mod:recv(Sock, Len, Timeout),
  {[ #kpro_Response{ responseMessage = #kpro_SaslHandshakeResponse{
                                          errorCode = ErrorCode }}],
    <<>>} = kpro:decode_response(<<Len:32, HandshakeResponseBin/binary>>),
  case ErrorCode of
    no_error ->
      ok = Mod:send(Sock, sasl_plain_token(SaslUser, SaslPassword)),
      case Mod:recv(Sock, 4, Timeout) of
        {ok, <<0:32>>} ->
          ok = setopts(Sock, Mod, [{active, once}]);
        {error, closed} ->
          exit({sasl_auth_error, bad_credentials});
        Unexpected ->
          exit({sasl_auth_error, Unexpected})
      end;
    _ -> exit({sasl_auth_error, ErrorCode})
  end;
maybe_sasl_auth(Host, Sock, Mod, _ClientId, Timeout,
    _SaslOpts = {_Method = kerberos, Keytab, Principal}) ->
    ?SASL_OK = sasl_auth:sasl_client_init(),
    {ok, _} = sasl_auth:kinit(Keytab, Principal),
    ok = setopts(Sock, Mod, [{active, false}]),
    case sasl_auth:sasl_client_new(<<"kafka">>, list_to_binary(Host), Principal) of
        ?SASL_OK ->
            sasl_auth:sasl_listmech(),
            CondFun = fun(?SASL_INTERACT) -> continue; (Other) -> Other end,
            StartCliFun = fun() ->
                {SaslRes, Token} = sasl_auth:sasl_client_start(),
                if
                    SaslRes >= 0 ->
                        send_sasl_token(Token, Sock, Mod),
                        SaslRes;
                    true ->
                        SaslRes
                end
            end,
            SaslRes =
                case do_while(StartCliFun, CondFun) of
                    SomeRes when SomeRes /= ?SASL_OK andalso SomeRes /= ?SASL_CONTINUE ->
                        exit({sasl_auth_error, SomeRes});
                    Other ->
                        Other
                end,
            case SaslRes of
                ?SASL_OK ->
                    ok = setopts(Sock, Mod, [{active, once}]);
                ?SASL_CONTINUE ->
                    sasl_recv(Mod, Sock, Timeout)
            end;
        Other ->
            exit({sasl_auth_error, Other})
    end.

sasl_recv(Mod, Sock, Timeout) ->
    case Mod:recv(Sock, 4, Timeout) of
        {ok, <<0:32>>} ->
            ok = setopts(Sock, Mod, [{active, once}]);
        {ok, <<BrokerTokenSize:32>>} ->
            case Mod:recv(Sock, BrokerTokenSize, Timeout) of
                {ok, BrokerToken} ->
                    CondFun = fun(?SASL_INTERACT) -> continue; (Other) -> Other end,
                    CliStepFun = fun() ->
                        {SaslRes, Token} = sasl_auth:sasl_client_step(BrokerToken),
                        if
                            SaslRes >= 0 ->
                                send_sasl_token(Token, Sock, Mod),
                                SaslRes;
                            true ->
                                SaslRes
                        end
                    end,
                    case do_while(CliStepFun, CondFun) of
                        ?SASL_OK ->
                            ok = setopts(Sock, Mod, [{active, once}]);
                        ?SASL_CONTINUE ->
                            sasl_recv(Mod, Sock, Timeout);
                        Other ->
                            exit({sasl_auth_error, Other})
                    end
            end;
        {error, closed} ->
            exit({sasl_auth_error, bad_credentials});
        Unexpected ->
            exit({sasl_auth_error, Unexpected})
    end.

do_while(Fun, CondFun) ->
    case CondFun(Fun()) of
        continue -> do_while(Fun, CondFun);
        Other -> Other
    end.

send_sasl_token(Challenge, Sock, Mod) when is_list(Challenge) ->
    ok = Mod:send(Sock, sasl_kerberos_token(list_to_binary(Challenge))).

sasl_plain_token(User, Password) ->
  Message = list_to_binary([0, unicode:characters_to_binary(User),
                            0, unicode:characters_to_binary(Password)]),
  <<(byte_size(Message)):32, Message/binary>>.

sasl_kerberos_token(Challenge) ->
    <<(byte_size(Challenge)):32, Challenge/binary>>.

setopts(Sock, _Mod = gen_tcp, Opts) -> inet:setopts(Sock, Opts);
setopts(Sock, _Mod = ssl, Opts)     ->  ssl:setopts(Sock, Opts).

system_call(Pid, Request) ->
  Mref = erlang:monitor(process, Pid),
  erlang:send(Pid, {system, {self(), Mref}, Request}),
  receive
    {Mref, Reply} ->
      erlang:demonitor(Mref, [flush]),
      Reply;
    {'DOWN', Mref, _, _, Reason} ->
      {error, {sock_down, Reason}}
  end.

call(Pid, Request) ->
  Mref = erlang:monitor(process, Pid),
  erlang:send(Pid, {{self(), Mref}, Request}),
  receive
    {Mref, Reply} ->
      erlang:demonitor(Mref, [flush]),
      Reply;
    {'DOWN', Mref, _, _, Reason} ->
      {error, {sock_down, Reason}}
  end.

reply({To, Tag}, Reply) ->
  To ! {Tag, Reply}.

loop(State, Debug) ->
  Msg = receive Input -> Input end,
  decode_msg(Msg, State, Debug).

decode_msg({system, From, Msg}, #state{parent = Parent} = State, Debug) ->
  sys:handle_system_msg(Msg, From, Parent, ?MODULE, Debug, State);
decode_msg(Msg, State, [] = Debug) ->
  handle_msg(Msg, State, Debug);
decode_msg(Msg, State, Debug0) ->
  Debug = sys:handle_debug(Debug0, fun print_msg/3, State, Msg),
  handle_msg(Msg, State, Debug).

handle_msg({_, Sock, Bin}, #state{ sock     = Sock
                                 , acc      = Acc0
                                 , requests = Requests
                                 , mod      = Mod
                                 } = State, Debug) when is_binary(Bin) ->
  case Mod of
    gen_tcp -> ok = inet:setopts(Sock, [{active, once}]);
    ssl     -> ok = ssl:setopts(Sock, [{active, once}])
  end,
  Acc1 = acc_recv_bytes(Acc0, Bin),
  {Responses, Acc} = decode_response(Acc1),
  NewRequests =
    lists:foldl(
      fun(#kpro_Response{ correlationId   = CorrId
                        , responseMessage = Response
                        }, Reqs) ->
        Caller = brod_kafka_requests:get_caller(Reqs, CorrId),
        cast(Caller, {msg, self(), CorrId, Response}),
        brod_kafka_requests:del(Reqs, CorrId)
      end, Requests, Responses),
  ?MODULE:loop(State#state{acc = Acc, requests = NewRequests}, Debug);
handle_msg(assert_max_req_age, #state{ requests = Requests
                                     , req_timeout = ReqTimeout
                                     } = State, Debug) ->
  SockPid = self(),
  erlang:spawn_link(fun() ->
                        ok = assert_max_req_age(Requests, ReqTimeout),
                        ok = send_assert_max_req_age(SockPid, ReqTimeout)
                    end),
  ?MODULE:loop(State, Debug);
handle_msg({tcp_closed, Sock}, #state{sock = Sock}, _) ->
  exit({shutdown, tcp_closed});
handle_msg({ssl_closed, Sock}, #state{sock = Sock}, _) ->
  exit({shutdown, ssl_closed});
handle_msg({tcp_error, Sock, Reason}, #state{sock = Sock}, _) ->
  exit({tcp_error, Reason});
handle_msg({ssl_error, Sock, Reason}, #state{sock = Sock}, _) ->
  exit({ssl_error, Reason});
handle_msg({From, {send, Request}},
           #state{ client_id = ClientId
                 , mod       = Mod
                 , sock      = Sock
                 , requests  = Requests
                 } = State, Debug) ->
  {Caller, _Ref} = From,
  {CorrId, NewRequests} =
    case Request of
      #kpro_ProduceRequest{requiredAcks = 0} ->
        brod_kafka_requests:increment_corr_id(Requests);
      _ ->
        brod_kafka_requests:add(Requests, Caller)
    end,
  RequestBin = kpro:encode_request(ClientId, CorrId, Request),
  Res = case Mod of
          gen_tcp -> gen_tcp:send(Sock, RequestBin);
          ssl     -> ssl:send(Sock, RequestBin)
        end,
  case Res of
    ok              -> reply(From, {ok, CorrId});
    {error, Reason} -> exit({send_error, Reason})
  end,
  ?MODULE:loop(State#state{requests = NewRequests}, Debug);
handle_msg({From, get_tcp_sock}, State, Debug) ->
  _ = reply(From, {ok, State#state.sock}),
  ?MODULE:loop(State, Debug);
handle_msg({From, stop}, #state{mod = Mod, sock = Sock}, _Debug) ->
  Mod:close(Sock),
  _ = reply(From, ok),
  ok;
handle_msg(Msg, #state{} = State, Debug) ->
  error_logger:warning_msg("[~p] ~p got unrecognized message: ~p",
                          [?MODULE, self(), Msg]),
  ?MODULE:loop(State, Debug).

cast(Pid, Msg) ->
  try
    Pid ! Msg,
    ok
  catch _ : _ ->
    ok
  end.

system_continue(_Parent, Debug, State) ->
  ?MODULE:loop(State, Debug).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _Parent, Debug, _Misc) ->
  sys:print_log(Debug),
  exit(Reason).

system_code_change(State, _Module, _Vsn, _Extra) ->
  {ok, State}.

format_status(Opt, Status) ->
  {Opt, Status}.

print_msg(Device, {_From, {send, Request}}, State) ->
  do_print_msg(Device, "send: ~p", [Request], State);
print_msg(Device, {tcp, _Sock, Bin}, State) ->
  do_print_msg(Device, "tcp: ~p", [Bin], State);
print_msg(Device, {tcp_closed, _Sock}, State) ->
  do_print_msg(Device, "tcp_closed", [], State);
print_msg(Device, {tcp_error, _Sock, Reason}, State) ->
  do_print_msg(Device, "tcp_error: ~p", [Reason], State);
print_msg(Device, {_From, stop}, State) ->
  do_print_msg(Device, "stop", [], State);
print_msg(Device, Msg, State) ->
  do_print_msg(Device, "unknown msg: ~p", [Msg], State).

do_print_msg(Device, Fmt, Args, State) ->
  CorrId = brod_kafka_requests:get_corr_id(State#state.requests),
  io:format(Device, "[~s] ~p [~10..0b] " ++ Fmt ++ "~n",
            [ts(), self(), CorrId] ++ Args).

ts() ->
  Now = os:timestamp(),
  {_, _, MicroSec} = Now,
  {{Y,M,D}, {HH,MM,SS}} = calendar:now_to_local_time(Now),
  lists:flatten(io_lib:format("~.4.0w-~.2.0w-~.2.0w ~.2.0w:~.2.0w:~.2.0w.~w",
                              [Y, M, D, HH, MM, SS, MicroSec])).

%% @private This is to be backward compatible for
%% 'timeout' as connect timeout option name
%% TODO: change to support 'connect_timeout' only for 2.3
%% @end
-spec get_connect_timeout(options()) -> timeout().
get_connect_timeout(Options) ->
  case {proplists:get_value(connect_timeout, Options),
        proplists:get_value(timeout, Options)} of
    {T, _} when is_integer(T) -> T;
    {_, T} when is_integer(T) -> T;
    _                         -> ?DEFAULT_CONNECT_TIMEOUT
  end.

%% @private Get request timeout from options.
-spec get_request_timeout(options()) -> timeout().
get_request_timeout(Options) ->
  proplists:get_value(request_timeout, Options, ?DEFAULT_REQUEST_TIMEOUT).

-spec assert_max_req_age(requests(), timeout()) -> ok | no_return().
assert_max_req_age(Requests, Timeout) ->
  case brod_kafka_requests:scan_for_max_age(Requests) of
    Age when Age > Timeout ->
      erlang:exit(request_timeout);
    _ ->
      ok
  end.

%% @private Send the 'assert_max_req_age' message to brod_sock process.
%% The send interval is set to a half of configured timeout.
%% @end
-spec send_assert_max_req_age(pid(), timeout()) -> ok.
send_assert_max_req_age(Pid, Timeout) when Timeout >= 1000 ->
  %% Check every 1 minute
  %% or every half of the timeout value if it's less than 2 minute
  SendAfter = erlang:min(Timeout div 2, timer:minutes(1)),
  _ = erlang:send_after(SendAfter, Pid, assert_max_req_age),
  ok.

%% @private Accumulate newly received bytes.
-spec acc_recv_bytes(acc(), binary()) -> acc().
acc_recv_bytes(Acc, NewBytes) when is_binary(Acc) ->
  case <<Acc/binary, NewBytes/binary>> of
    <<Size:32/signed-integer, _/binary>> = AccBytes ->
      do_acc(#acc{expected_size = Size + ?SIZE_HEAD_BYTES}, AccBytes);
    AccBytes ->
      AccBytes
  end;
acc_recv_bytes(#acc{} = Acc, NewBytes) ->
  do_acc(Acc, NewBytes).

%% @private Add newly received bytes to buffer.
-spec do_acc(acc(), binary()) -> acc().
do_acc(#acc{acc_size = AccSize, acc_buffer = AccBuffer} = Acc, NewBytes) ->
  Acc#acc{acc_size = AccSize + size(NewBytes),
          acc_buffer = [NewBytes | AccBuffer]
         }.

%% @private Decode response when accumulated enough bytes.
-spec decode_response(acc()) -> {[kpro_Response()], acc()}.
decode_response(#acc{expected_size = ExpectedSize,
                     acc_size = AccSize,
                     acc_buffer = AccBuffer}) when AccSize >= ExpectedSize ->
  %% iolist_to_binary here to simplify kafka_protocol implementation
  %% maybe make it smarter in the next version
  kpro:decode_response(iolist_to_binary(lists:reverse(AccBuffer)));
decode_response(Acc) ->
  {[], Acc}.

%%%_* Eunit ====================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

acc_test_() ->
  [{"clean start flow",
    fun() ->
        Acc0 = acc_recv_bytes(<<>>, <<0, 0>>),
        ?assertEqual(Acc0, <<0, 0>>),
        Acc1 = acc_recv_bytes(Acc0, <<0, 1, 0, 0>>),
        ?assertEqual(#acc{expected_size = 5,
                          acc_size = 6,
                          acc_buffer = [<<0, 0, 0, 1, 0, 0>>]
                         }, Acc1)
    end},
   {"old tail leftover",
    fun() ->
        Acc0 = acc_recv_bytes(<<0, 0>>, <<0, 4>>),
        ?assertEqual(#acc{expected_size = 8,
                          acc_size = 4,
                          acc_buffer = [<<0, 0, 0, 4>>]
                         }, Acc0),
        Acc1 = acc_recv_bytes(Acc0, <<0, 0>>),
        ?assertEqual(#acc{expected_size = 8,
                          acc_size = 6,
                          acc_buffer = [<<0, 0>>, <<0, 0, 0, 4>>]
                         }, Acc1),
        Acc2 = acc_recv_bytes(Acc1, <<1, 1>>),
        ?assertEqual(#acc{expected_size = 8,
                          acc_size = 8,
                          acc_buffer = [<<1, 1>>, <<0, 0>>, <<0, 0, 0, 4>>]
                         }, Acc2)
    end
   }
  ].

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
