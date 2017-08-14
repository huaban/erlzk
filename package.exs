defmodule Erlzk.MixFile do
  use Mix.Project

  def project do
    [app: :erlzk,
     version: "0.6.4",
     description: "A Pure Erlang ZooKeeper Client (no C dependency)",
     package: package]
  end

  def package do
    [files: ~w(src include rebar.config README.md LICENSE Makefile),
     licenses: ["Apache 2.0"],
     maintainers: ["Mega Yu", "Belltoy Zhao"],
     links: %{"GitHub" => "https://github.com/huaban/erlzk"}]
  end
end
