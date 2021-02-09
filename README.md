haskey
======

[![Hackage](https://img.shields.io/hackage/v/haskey.svg?maxAge=2592000)](https://hackage.haskell.org/package/haskey)
[![Stackage Nightly](http://stackage.org/package/haskey/badge/nightly)](http://stackage.org/nightly/package/haskey)
[![Stackage LTS](http://stackage.org/package/haskey/badge/lts)](http://stackage.org/lts/package/haskey)

Haskey is a transactional, ACID compliant, embeddable, scalable key-value
store written entirely in Haskell. It was developed as part of the [Summer of Haskell 2017][soh2017] project.

  [soh2017]: https://summer.haskell.org/news/2017-05-24-accepted-projects.html

## Tutorial

A full tutorial can be [found in the haskey-mtl library](https://github.com/haskell-haskey/haskey-mtl/blob/master/docs/tutorial.md), along with a [full code example](https://github.com/haskell-haskey/haskey-mtl/tree/master/example).

## Historical blog posts
Some blog posts have been written on Haskey's design an internals. These give an insight in the inner workings of Haskey, but the used APIs might be a bit outdated.

  - An introductory blog post on Haskey can be found [here][introduction].
  - An blog post on user-defined schemas and using the `HaskeyT` monad can be found [here][haskey-mtl-post]

  [introduction]: https://hverr.github.io/posts/2017-08-24-introducing-haskey.html
  [haskey-mtl-post]: https://hverr.github.io/posts/2017-09-14-haskey-user-defined-schemas-and-monad-transformers.html
