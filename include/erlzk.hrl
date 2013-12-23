-define(ZK_ID_ANYONE_ID_UNSAFE, ["world", "anyone"]).
-define(ZK_ID_AUTH_IDS, ["auth", ""]).

-define(ZK_ACL_OPEN_ACL_UNSAFE, list_to_tuple([rwcda|?ZK_ID_ANYONE_ID_UNSAFE])).
-define(ZK_ACL_CREATOR_ALL_ACL, list_to_tuple([rwcda|?ZK_ID_AUTH_IDS])).
-define(ZK_ACL_READ_ACL_UNSAFE, list_to_tuple([r|?ZK_ID_ANYONE_ID_UNSAFE])).

-record(stat, {czxid, mzxid, ctime, mtime, version, cversion, aversion, ephemeral_owner, data_length, num_children, pzxid}).