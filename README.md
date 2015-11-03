# erlzk

**erlzk** is A Pure Erlang ZooKeeper Client (no C dependency).

> NOTE: You should be familiar with Zookeeper and have read the
[Zookeeper Programmers Guide](https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html)
before using erlzk.

> ATTENTION: ZooKeeper latest stable v3.4.6 removed create2 function, 
  but it works fine in v3.4.5 or current developing v3.5.0 (ZooKeeper added it again).

## Features

- Clear and concise API
- Connection state changing monitor
- Data and child watchers
- Support for latest Zookeeper servers
- Pure Erlang based implementation of the wire protocol

## Installation

Download the sources from our [Github repository](http://github.com/huaban/erlzk)

To build the application simply run `make`. This should build .beam, .app files.

To run tests, run `make test`.

To generate doc, run `make doc`.

Or add it to your rebar config

```erlang
{deps, [
    ....
    {erlzk, ".*", {git, "git://github.com/huaban/erlzk.git", {branch, "master"}}}
]}.
```

## Basic Usage

The basic usage of erlzk is:

### Start erlzk

erlzk is an
[OTP](http://www.erlang.org/doc/design_principles/users_guide.html)
application. You have to start it first before using any of the functions.

To start in the console run:

```
$ erl -pa ebin
1>> erlzk:start().
ok
```

It will start erlzk and all of the application it depends on:

```erlang
application:start(crypto),
application:start(erlzk).
```

Or add erlzk to the applications property of your .app in a release.

### Connect to ZooKeeper

Begin to leverage the ZooKeeper, you need to connect to it first.

The simplest way is use `erlzk:connect/2`,
the parameters are ServerList and Timeout, the format of ServerList are [{Host, Port}],
the server list will be shuffled every time erlzk make a connection to one of the servers,
timeout is used by the ZooKeeper cluster to determine when the client's session expires,
the valid value is 2 to 20 times of the ticktime, if it's not in this range,
ZooKeeper will pick a proper one for you, in order to keep the connection erlzk will send
a ping message if no other message sent in a third of negotiated timeout.

```erlang
{ok, Pid} = erlzk:connect([{"localhost", 2181}], 30000).
```

Or you can use `erlzk:connect/3` or `erlzk:connect/4` for more options,
the options parameter is a `proplists`, support 4 option types:

+ chroot: specify a node under which erlzk will operate on
+ disable_watch_auto_reset: whether disable reset watches after the session timeout, default is false
+ auth_data: the auths need to be added after connected
+ monitor: a process receiving the message of connection state changing

If you set monitor, each time erlzk is disconnected or reconnected or (reconnected after) session expired,
will send monitor a message in the form of {State, Host, Port}, there are 3 states:

+ disconnected
+ connected
+ expired

```erlang
{ok, Pid} = erlzk:connect([{"localhost", 2181}], 30000, [{chroot, "/zk"},
														 {auth_data, [{"digest", <<"foo:bar">>}],
                                                         {monitor, Monitor}]).
```

> NOTE: After session expired, erlzk can reset watches if disable_watch_auto_reset is false,
but all ephemeral nodes you created will be deleted by ZooKeeper,
it's your program's duty to rebuild it, so it's highly recommended to use monitor.

Once connected, erlzk will attempt to stay connected regardless of intermittent connection loss
or Zookeeper session expiration. Your program can be instructed to drop a connection by calling `erlzk:close/1`:

```erlang
erlzk:close(Pid).
```

### Commuticate with ZooKeeper

ZooKeeper has several API to commuticate with. Below is a simple example,
more details see module erlzk or test.

```erlang
% Include the hrl first
-include_lib("erlzk/include/erlzk.hrl").

% Create a node with the given path, return the actual path of the node
{ok, "/a"} = erlzk:create(Pid, "/a").

% Determine if a node exists, return the stat of the node
{ok, Stat} = erlzk:exists(Pid, "/a").

% Update the data for a given node, use the current version of the node for data security
{ok, _Stat} = erlzk:set_data(Pid, "/a", <<"b">>, Stat#stat.version).

% Get the data of the node
{ok, {<<"b">>, _Stat}} = erlzk:get_data(Pid, "/a").

% Add a auth, username is "foo", password is "bar"
ok = erlzk:add_auth(Pid, "foo", "bar").

% Set the ACL of the node, now only the creator has all the permissions
{ok, _Stat} = erlzk:set_acl(Pid, "/a", ?ZK_ACL_CREATOR_ALL_ACL).

% Get the ACL of the node, Acl should equals to [{rwcdr,"digest",erlzk:generate_digest("foo", "bar")}]
{ok, {Acl, _Stat}} = erlzk:get_acl(Pid, "/a")).

% Create some children of the node
{ok, "/a/a0000000000"} = erlzk:create(P, "/a/a", persistent_sequential).
{ok, "/a/b"} = erlzk:create(P, "/a/b").

% Get the children of the node, Children should include "a0000000000" and "b"
{ok, Children} = erlzk:get_children(P, "/a").

% Delete the node, delete all the children before parent
ok = erlzk:delete(Pid, "/a/a0000000000").
ok = erlzk:delete(Pid, "/a/b").
ok = erlzk:delete(Pid, "/a").

```

### Set Watchers

Some functions like`erlzk:exists/3`, `erlzk:get_data/3`, `erlzk:get_children/3`, `erlzk:get_children2/3`
accept a watcher, it's your program's process, used for receiving ZooKeeper watch events.

A successful `erlzk:create/5` will trigger all the watches left on the
node of the given path by `erlzk:exists/3` and the watches left on the parent
node by `erlzk:get_children/3`.

> NOTE: ZooKeeper official client implementation is wrong. In its client,
> NodeCreated will trigger both the DataWatches and the ExistWatches, but
> ZooKeeper server won't send created event for DataWatches (`get_data`).

A successful `erlzk:delete/3` will trigger all the watches left on the node of
the given path by `erlzk:exists/3` and `erlzk:get_data/3` and `erlzk:get_children/3`,
and the watches left on the parent node by `erlzk:get_children/3`.

A successful `erlzk:set_data/4` will trigger all the watches on the node of the given path
left by `erlzk:exists/3` and `erlzk:get_data/3` calls.

When erlzk receive a watch event will send a message to your watcher,
the message is a tuple, in the form of {RegisterOperate, RegisterPath, WatchEvent},
RegisterOperate and RegisterPath is useful when you need to reset a new watcher,
WatchEvent include:

+ node_created
+ node_deleted
+ node_data_changed
+ node_children_changed

> NOTE: ZooKeeper watch event is one-time trigger, for more details about watch, read
[ZooKeeper Watches](https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkWatches).

Simple example as follows:

```erlang
% set an exists watch
erlzk:exists(Pid, "/a", spawn(fun() ->
        receive
            % receive a watch event
            {Op, Path, Event} ->
                Op = exists,
                Path = "/a",
                Event = node_created
        end
    end)),
% create a node trigger the watch
{ok, "/a"} = erlzk:create(Pid, "/a").

```

## API Specification

See erlzk.erl for more details, all the functions you need are in this module.

## Contribute

- Fork this repository on github
- Make your changes and send us a pull request
- If we like them we'll merge them

## License

Copyright (c) 2013 Mega Yu & Huaban.com. Distributed under the Apache License 2.0. See LICENSE for further details.
