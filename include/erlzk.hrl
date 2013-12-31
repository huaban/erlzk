% This Id represents anyone.
-define(ZK_ID_ANYONE_ID_UNSAFE, ["world", "anyone"]).
% This Id is only usable to set ACLs. It will get substituted with the Id's the client authenticated with.
-define(ZK_ID_AUTH_IDS, ["auth", ""]).

% This is a completely open ACL.
-define(ZK_ACL_OPEN_ACL_UNSAFE, list_to_tuple([rwcda|?ZK_ID_ANYONE_ID_UNSAFE])).
% This ACL gives the creators authentication id's all permissions.
-define(ZK_ACL_CREATOR_ALL_ACL, list_to_tuple([rwcda|?ZK_ID_AUTH_IDS])).
% This ACL gives the world the ability to read.
-define(ZK_ACL_READ_ACL_UNSAFE, list_to_tuple([r|?ZK_ID_ANYONE_ID_UNSAFE])).

-record(stat, {
    czxid           :: non_neg_integer(),   % The zxid of the change that caused this znode to be created.
    mzxid           :: non_neg_integer(),   % The zxid of the change that last modified this znode.
    ctime           :: non_neg_integer(),   % The time in milliseconds from epoch when this znode was created.
    mtime           :: non_neg_integer(),   % The time in milliseconds from epoch when this znode was last modified.
    version         :: non_neg_integer(),   % The number of changes to the data of this znode.
    cversion        :: non_neg_integer(),   % The number of changes to the children of this znode.
    aversion        :: non_neg_integer(),   % The number of changes to the ACL of this znode.
    ephemeral_owner :: non_neg_integer(),   % The session id of the owner of this znode if the znode is an ephemeral node.
                                            % If it is not an ephemeral node, it will be zero.
    data_length     :: non_neg_integer(),   % The length of the data field of this znode.
    num_children    :: non_neg_integer(),   % The number of children of this znode.
    pzxid           :: non_neg_integer()    % The zxid of the change that last created or deleted the children of this znode.
}).