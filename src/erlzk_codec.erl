-module(erlzk_codec).

-include_lib("../include/erlzk.hrl").

-export([pack/2, pack/3, pack/4, unpack/1, unpack/2, unpack/3]).

-define(ZK_PERM_READ, 1).   % can read node’s value and list its children
-define(ZK_PERM_WRITE, 2).  % can set the node’s value
-define(ZK_PERM_CREATE, 4). % can create children
-define(ZK_PERM_DELETE, 8). % can delete children
-define(ZK_PERM_ADMIN, 16). % can execute set_acl

-define(ZK_EVENT_TYPE_NONE, -1).
-define(ZK_EVENT_TYPE_NODE_CREATED, 1).
-define(ZK_EVENT_TYPE_NODE_DELETED, 2).
-define(ZK_EVENT_TYPE_NODE_DATA_CHANGED, 3).
-define(ZK_EVENT_TYPE_NODE_CHILDREN_CHANGED, 4).

-define(ZK_KEEPER_STATE_DISCONNECTED, 0).
-define(ZK_KEEPER_STATE_SYNC_CONNECTED, 3).
-define(ZK_KEEPER_STATE_AUTH_FAILED, 4).
-define(ZK_KEEPER_STATE_CONNECTED_READ_ONLY, 5).
-define(ZK_KEEPER_STATE_SASL_AUTHENTICATED, 6).
-define(ZK_KEEPER_STATE_EXPIRED, -112).

-define(ZK_OP_CREATE, 1).
-define(ZK_OP_DELETE, 2).
-define(ZK_OP_EXISTS, 3).
-define(ZK_OP_GET_DATA, 4).
-define(ZK_OP_SET_DATA, 5).
-define(ZK_OP_GET_ACL, 6).
-define(ZK_OP_SET_ACL, 7).
-define(ZK_OP_GET_CHILDREN, 8).
-define(ZK_OP_SYNC, 9).
-define(ZK_OP_GET_CHILDREN2, 12).
-define(ZK_OP_CHECK, 13).
-define(ZK_OP_MULTI, 14).
-define(ZK_OP_CREATE2, 15).
-define(ZK_OP_RECONFIG, 16).
-define(ZK_OP_AUTH, 100).
-define(ZK_OP_SET_WATCHES, 101).
-define(ZK_OP_SASL, 102).
-define(ZK_OP_ERROR, -1).

-define(ZK_CODE_OK, 0).
-define(ZK_CODE_SYSTEM_ERROR, -1).
-define(ZK_CODE_RUNTIME_INCONSISTENCY, -2).
-define(ZK_CODE_DATA_INCONSISTENCY, -3).
-define(ZK_CODE_CONNECTION_LOSS, -4).
-define(ZK_CODE_MARSHALLING_ERROR, -5).
-define(ZK_CODE_UNIMPLEMENTED, -6).
-define(ZK_CODE_OPERATION_TIMEOUT, -7).
-define(ZK_CODE_BAD_ARGUMENTS, -8).
-define(ZK_CODE_UNKNOWN_SESSION, -12).
-define(ZK_CODE_API_ERROR, -100).
-define(ZK_CODE_NO_NODE, -101).
-define(ZK_CODE_NO_AUTH, -102).
-define(ZK_CODE_BAD_VERSION, -103).
-define(ZK_CODE_NO_CHILDREN_FOR_EPHEMERALS, -108).
-define(ZK_CODE_NODE_EXISTS, -110).
-define(ZK_CODE_NOT_EMPTY, -111).
-define(ZK_CODE_SESSION_EXPIRED, -112).
-define(ZK_CODE_INVALID_CALLBACK, -113).
-define(ZK_CODE_INVALID_ACL, -114).
-define(ZK_CODE_AUTH_FAILED, -115).
-define(ZK_CODE_SESSION_MOVED, -118).
-define(ZK_CODE_NOT_READ_ONLY, -119).
-define(ZK_CODE_NEW_CONFIG_NO_QUORUM, -120).
-define(ZK_CODE_RECONFIG_IN_PROGRESS, -121).
-define(ZK_CODE_EPHEMERAL_ON_LOCAL_SESSION, -122).

%% ===================================================================
%% Public API
%% ===================================================================
pack(connect, {ProtocolVersion, LastZxidSeen, Timeout, SessionId, Password}) ->
    <<ProtocolVersion:32, LastZxidSeen:64, Timeout:32, SessionId:64, (pack_bytes(Password))/binary>>.

pack(add_auth, {Scheme, Auth}, Xid) ->
    Packet = <<0:32, (pack_str(Scheme))/binary, (pack_bytes(Auth))/binary>>,
    wrap_packet(?ZK_OP_AUTH, Xid, Packet);

pack(set_watches, {LastZxidSeen, DataWatches, ExistWatches, ChildWatches}, Xid) ->
    Packet = <<LastZxidSeen:64, (pack_watches(DataWatches))/binary, (pack_watches(ExistWatches))/binary, (pack_watches(ChildWatches))/binary>>,
    wrap_packet(?ZK_OP_SET_WATCHES, Xid, Packet).

pack(create, {Path, Data, Acl, CreateMode}, Xid, Chroot) ->
    Packet = <<(pack_str(chroot(Path, Chroot)))/binary, (pack_bytes(Data))/binary, (pack_acl(Acl))/binary, (pack_create_mode(CreateMode))/binary>>,
    wrap_packet(?ZK_OP_CREATE, Xid, Packet);

pack(delete, {Path, Version}, Xid, Chroot) ->
    Packet = <<(pack_str(chroot(Path, Chroot)))/binary, Version:32/signed>>,
    wrap_packet(?ZK_OP_DELETE, Xid, Packet);

pack(exists, {Path, true}, Xid, Chroot) ->
    pack(exists, {Path, 1}, Xid, Chroot);
pack(exists, {Path, false}, Xid, Chroot) ->
    pack(exists, {Path, 0}, Xid, Chroot);
pack(exists, {Path, Watch}, Xid, Chroot) ->
    Packet = <<(pack_str(chroot(Path, Chroot)))/binary, Watch:8>>,
    wrap_packet(?ZK_OP_EXISTS, Xid, Packet);

pack(get_data, {Path, true}, Xid, Chroot) ->
    pack(get_data, {Path, 1}, Xid, Chroot);
pack(get_data, {Path, false}, Xid, Chroot) ->
    pack(get_data, {Path, 0}, Xid, Chroot);
pack(get_data, {Path, Watch}, Xid, Chroot) ->
    Packet = <<(pack_str(chroot(Path, Chroot)))/binary, Watch:8>>,
    wrap_packet(?ZK_OP_GET_DATA, Xid, Packet);

pack(set_data, {Path, Data, Version}, Xid, Chroot) ->
    Packet = <<(pack_str(chroot(Path, Chroot)))/binary, (pack_bytes(Data))/binary, Version:32/signed>>,
    wrap_packet(?ZK_OP_SET_DATA, Xid, Packet);

pack(get_acl, {Path}, Xid, Chroot) ->
    Packet = <<(pack_str(chroot(Path, Chroot)))/binary>>,
    wrap_packet(?ZK_OP_GET_ACL, Xid, Packet);

pack(set_acl, {Path, Acl, Version}, Xid, Chroot) ->
    Packet = <<(pack_str(chroot(Path, Chroot)))/binary, (pack_acl(Acl))/binary, Version:32/signed>>,
    wrap_packet(?ZK_OP_SET_ACL, Xid, Packet);

pack(get_children, {Path, true}, Xid, Chroot) ->
    pack(get_children, {Path, 1}, Xid, Chroot);
pack(get_children, {Path, false}, Xid, Chroot) ->
    pack(get_children, {Path, 0}, Xid, Chroot);
pack(get_children, {Path, Watch}, Xid, Chroot) ->
    Packet = <<(pack_str(chroot(Path, Chroot)))/binary, Watch:8>>,
    wrap_packet(?ZK_OP_GET_CHILDREN, Xid, Packet);

pack(sync, {Path}, Xid, Chroot) ->
    Packet = <<(pack_str(chroot(Path, Chroot)))/binary>>,
    wrap_packet(?ZK_OP_SYNC, Xid, Packet);

pack(get_children2, {Path, true}, Xid, Chroot) ->
    pack(get_children2, {Path, 1}, Xid, Chroot);
pack(get_children2, {Path, false}, Xid, Chroot) ->
    pack(get_children2, {Path, 0}, Xid, Chroot);
pack(get_children2, {Path, Watch}, Xid, Chroot) ->
    Packet = <<(pack_str(chroot(Path, Chroot)))/binary, Watch:8>>,
    wrap_packet(?ZK_OP_GET_CHILDREN2, Xid, Packet);

pack(create2, {Path, Data, Acl, CreateMode}, Xid, Chroot) ->
    Packet = <<(pack_str(chroot(Path, Chroot)))/binary, (pack_bytes(Data))/binary, (pack_acl(Acl))/binary, (pack_create_mode(CreateMode))/binary>>,
    wrap_packet(?ZK_OP_CREATE2, Xid, Packet).

unpack(Packet) ->
    <<Xid:32/signed, Zxid:64, Code:32/signed, Body/binary>> = Packet,
    {Xid, Zxid, code_to_atom(Code), Body}.

unpack(connect, Packet) ->
    <<ProtocolVersion:32, TimeOut:32, SessionId:64, Left/binary>> = Packet,
    {Password, _}= unpack_bytes(Left),
    {ProtocolVersion, TimeOut, SessionId, Password}.

unpack(create, Packet, Chroot) ->
    {Path, _} = unpack_str(Packet),
    unchroot(Path, Chroot);

unpack(exists, Packet, _Chroot) ->
    unpack_stat(Packet);

unpack(get_data, Packet, _Chroot) ->
    {Data, Left} = unpack_bytes(Packet),
    {Data, unpack_stat(Left)};

unpack(set_data, Packet, _Chroot) ->
    unpack_stat(Packet);

unpack(get_acl, Packet, _Chroot) ->
    {Acl, Left} = unpack_acl(Packet),
    {Acl, unpack_stat(Left)};

unpack(set_acl, Packet, _Chroot) ->
    unpack_stat(Packet);

unpack(get_children, Packet, _Chroot) ->
    {Children, _} = unpack_strs(Packet),
    Children;

unpack(sync, Packet, Chroot) ->
    {Path, _} = unpack_str(Packet),
    unchroot(Path, Chroot);

unpack(get_children2, Packet, _Chroot) ->
    {Children, Left} = unpack_strs(Packet),
    {Children, unpack_stat(Left)};

unpack(create2, Packet, Chroot) ->
    {Path, Left} = unpack_str(Packet),
    {unchroot(Path, Chroot), unpack_stat(Left)};

unpack(watched_event, Packet, Chroot) ->
    <<Type:32/signed, State:32/signed, Left/binary>> = Packet,
    {Path, _} = unpack_str(Left),
    {event_type_to_atom(Type), keeper_state_to_atom(State), unchroot(Path, Chroot)}.

%% ===================================================================
%% Internal Functions
%% ===================================================================
chroot(Path, Chroot) ->
    case Chroot of
        "/" -> Path;
        ""  -> Path;
        _   -> Chroot ++ Path
    end.

unchroot(Path, Chroot) ->
    case Chroot of
        "/" -> Path;
        ""  -> Path;
        _   -> string:substr(Path, string:len(Chroot) + 1)
    end.

pack_str(Str) ->
    Length = iolist_size(Str),
    if Length =:= 0 -> <<-1:32/signed>>;
       Length >   0 -> <<Length:32, (iolist_to_binary(Str))/binary>>
    end.

pack_bytes(Bytes) ->
    Length = size(Bytes),
    if Length =:= 0 -> <<-1:32/signed>>;
       Length >   0 -> <<Length:32, Bytes/binary>>
    end.

pack_acl(Acl) ->
    pack_acl(Acl, <<>>, 0).

pack_acl([], Packet, Size) ->
    case Size of
        0 -> <<-1:32/signed>>;
        _ -> <<Size:32, Packet/binary>>
    end;
pack_acl([{Perms,Scheme,Id}|Left], Packet, Size) ->
    NewPacket = <<Packet/binary, (pack_perms(Perms)):32, (pack_str(Scheme))/binary, (pack_str(Id))/binary>>,
    pack_acl(Left, NewPacket, Size + 1).

pack_create_mode(CreateMode) ->
    Flags = case CreateMode of
        persistent -> 0;
        p -> 0;
        ephemeral  -> 1;
        e -> 1;
        persistent_sequential -> 2;
        ps -> 2;
        ephemeral_sequential  -> 3;
        es -> 3;
        _ -> 0
    end,
    <<Flags:32>>.

pack_perms(Perms) ->
    pack_perms(atom_to_list(Perms), 0).

pack_perms([], PermsValue) ->
    PermsValue;
pack_perms([Perm|Left], PermsValue) ->
    Value = case Perm of
        $r -> ?ZK_PERM_READ;
        $w -> ?ZK_PERM_WRITE;
        $c -> ?ZK_PERM_CREATE;
        $d -> ?ZK_PERM_DELETE;
        $a -> ?ZK_PERM_ADMIN;
        _ -> 0
    end,
    pack_perms(Left, (PermsValue bor Value)).

pack_watches(Watches) ->
    pack_watches(Watches, <<>>, 0).

pack_watches([], Packet, Size) ->
    case Size of
        0 -> <<0:32/signed>>;
        _ -> <<Size:32, Packet/binary>>
    end;
pack_watches([Watch|Left], Packet, Size) ->
    NewPacket = <<Packet/binary, (pack_str(Watch))/binary>>,
    pack_watches(Left, NewPacket, Size + 1).

wrap_packet(Type, Xid, Packet) ->
    <<Xid:32/signed, Type:32, Packet/binary>>.

unpack_str(Packet) ->
    <<Length:32/signed, Left/binary>> = Packet,
    if Length =< 0  ->
        {"", Left};
       Length >  0  ->
        {Str, LeftData} = split_binary(Left, Length),
        {binary_to_list(Str), LeftData}
    end.

unpack_strs(Packet) ->
    <<Size:32/signed, Left/binary>> = Packet,
    unpack_strs(Left, [], Size).

unpack_strs(Packet, Strs, Size) when Size =< 0 ->
    {Strs, Packet};
unpack_strs(Packet, Strs, Size) ->
    {Str, Left} = unpack_str(Packet),
    unpack_strs(Left, [Str|Strs], Size - 1).

unpack_bytes(Packet) ->
    <<Length:32/signed, Left/binary>> = Packet,
    if Length =< 0  ->
        {<<>>, Left};
       Length >  0  ->
        split_binary(Left, Length)
    end.

unpack_acl(Packet) ->
    <<Size:32, Left/binary>> = Packet,
    unpack_acl([], Left, Size).

unpack_acl(Acl, Packet, 0) ->
    {Acl, Packet};
unpack_acl(Acl, Packet, Size) ->
    <<PermsPacket:32, Left/binary>> = Packet,
    Perms = unpack_perms(PermsPacket),
    {Scheme, Left0} = unpack_str(Left),
    {Id,     Left1} = unpack_str(Left0),
    unpack_acl([{Perms,Scheme,Id}|Acl], Left1, Size - 1).
    
unpack_perms(PermsValue) ->
    unpack_perms(PermsValue, [?ZK_PERM_READ,"r",?ZK_PERM_WRITE,"w",?ZK_PERM_CREATE,"c",?ZK_PERM_DELETE,"d",?ZK_PERM_ADMIN,"r"], "").

unpack_perms(_PermsValue, [], Perms) ->
    list_to_atom(Perms);
unpack_perms(PermsValue, [V,P|Left], Perms) ->
    NewPerms = if (PermsValue band V) =:= V ->
            Perms ++ P;
        true ->
            Perms
    end,
    unpack_perms(PermsValue, Left, NewPerms).

unpack_stat(Packet) ->
    <<Czxid:64, Mzxid:64, Ctime:64, Mtime:64, Version:32, Cversion:32, Aversion:32, EphemeralOwner:64, DataLength:32, NumChildren:32, Pzxid:64>> = Packet,
    #stat{czxid = Czxid,
        mzxid = Mzxid,
        ctime = Ctime,
        mtime = Mtime,
        version = Version,
        cversion = Cversion,
        aversion = Aversion,
        ephemeral_owner = EphemeralOwner,
        data_length = DataLength,
        num_children = NumChildren,
        pzxid = Pzxid}.

event_type_to_atom(Type) ->
    case Type of
        ?ZK_EVENT_TYPE_NONE -> none;
        ?ZK_EVENT_TYPE_NODE_CREATED -> node_created;
        ?ZK_EVENT_TYPE_NODE_DELETED -> node_deleted;
        ?ZK_EVENT_TYPE_NODE_DATA_CHANGED -> node_data_changed;
        ?ZK_EVENT_TYPE_NODE_CHILDREN_CHANGED -> node_children_changed
    end.

keeper_state_to_atom(State) ->
    case State of
        ?ZK_KEEPER_STATE_DISCONNECTED -> disconnected;
        ?ZK_KEEPER_STATE_SYNC_CONNECTED -> sync_connected;
        ?ZK_KEEPER_STATE_AUTH_FAILED -> auth_failed;
        ?ZK_KEEPER_STATE_CONNECTED_READ_ONLY -> connected_read_only;
        ?ZK_KEEPER_STATE_SASL_AUTHENTICATED -> sasl_authenticated;
        ?ZK_KEEPER_STATE_EXPIRED -> expired
    end.

code_to_atom(Code) ->
    case Code of
        ?ZK_CODE_OK -> ok;
        ?ZK_CODE_SYSTEM_ERROR -> system_error;
        ?ZK_CODE_RUNTIME_INCONSISTENCY -> runtime_inconsistency;
        ?ZK_CODE_DATA_INCONSISTENCY -> data_inconsistency;
        ?ZK_CODE_CONNECTION_LOSS -> connection_loss;
        ?ZK_CODE_MARSHALLING_ERROR -> marshalling_error;
        ?ZK_CODE_UNIMPLEMENTED -> unimplemented;
        ?ZK_CODE_OPERATION_TIMEOUT -> operation_timeout;
        ?ZK_CODE_BAD_ARGUMENTS -> bad_arguments;
        ?ZK_CODE_UNKNOWN_SESSION -> unknown_session;
        ?ZK_CODE_API_ERROR -> api_error;
        ?ZK_CODE_NO_NODE -> no_node;
        ?ZK_CODE_NO_AUTH -> no_auth;
        ?ZK_CODE_BAD_VERSION -> bad_version;
        ?ZK_CODE_NO_CHILDREN_FOR_EPHEMERALS -> no_children_for_ephemerals;
        ?ZK_CODE_NODE_EXISTS -> node_exists;
        ?ZK_CODE_NOT_EMPTY -> not_empty;
        ?ZK_CODE_SESSION_EXPIRED -> session_expired;
        ?ZK_CODE_INVALID_CALLBACK -> invalid_callback;
        ?ZK_CODE_INVALID_ACL -> invalid_acl;
        ?ZK_CODE_AUTH_FAILED -> auth_failed;
        ?ZK_CODE_SESSION_MOVED -> session_moved;
        ?ZK_CODE_NOT_READ_ONLY -> not_read_only;
        ?ZK_CODE_NEW_CONFIG_NO_QUORUM -> new_config_no_quorum;
        ?ZK_CODE_RECONFIG_IN_PROGRESS -> reconfig_in_progress;
        ?ZK_CODE_EPHEMERAL_ON_LOCAL_SESSION -> ephemeral_on_local_session
    end.
