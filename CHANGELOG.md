# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.3](https://github.com/band-ai/phoenix-channels-python-client/compare/phoenix-channels-python-client-v0.2.2...phoenix-channels-python-client-v0.2.3) (2026-07-23)


### Bug Fixes

* match release-please component-prefixed tag in publish workflow ([#51](https://github.com/band-ai/phoenix-channels-python-client/issues/51)) ([dedb10c](https://github.com/band-ai/phoenix-channels-python-client/commit/dedb10cce3d60fa7a5cf62abec4ebcbe5323d317))


### Miscellaneous Chores

* prepare repo for band-ai org transfer and rename ([#48](https://github.com/band-ai/phoenix-channels-python-client/issues/48)) ([5298877](https://github.com/band-ai/phoenix-channels-python-client/commit/5298877cf96f5c0e976c29721ed6d2da2543e32b))

## [0.2.2](https://github.com/thenvoi/phoenix-channels-python-client-alpha/compare/phoenix-channels-python-client-alpha-v0.2.1...phoenix-channels-python-client-alpha-v0.2.2) (2026-07-20)


### Features

* send optional additional_headers on the WebSocket handshake ([#46](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/46)) ([c951d96](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/c951d96a669b1842f3aa432ad0132e151a0c674f))

## [0.2.1](https://github.com/thenvoi/phoenix-channels-python-client-alpha/compare/phoenix-channels-python-client-alpha-v0.2.0...phoenix-channels-python-client-alpha-v0.2.1) (2026-04-12)


### Bug Fixes

* **deps:** bump cryptography from 46.0.5 to 46.0.7 in /.github/actions/GithubToken ([#34](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/34)) ([5c69b77](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/5c69b7776b871b8ebd083596d798d16c94035c42))
* **deps:** bump cryptography in /.github/actions/GithubToken ([5c69b77](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/5c69b7776b871b8ebd083596d798d16c94035c42))
* **deps:** bump requests from 2.32.5 to 2.33.1 in /.github/actions/GithubToken ([#32](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/32)) ([dba44bf](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/dba44bf42273c07c86db6ee921b3760e86a90106))
* **deps:** bump requests in /.github/actions/GithubToken ([dba44bf](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/dba44bf42273c07c86db6ee921b3760e86a90106))

## [0.2.0](https://github.com/thenvoi/phoenix-channels-python-client-alpha/compare/phoenix-channels-python-client-alpha-v0.1.5...phoenix-channels-python-client-alpha-v0.2.0) (2026-03-06)


### ⚠ BREAKING CHANGES

* **websocket:** reconnect cascade under shared agent contention ([#30](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/30))

### Bug Fixes

* **websocket:** reconnect cascade under shared agent contention ([#30](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/30)) ([e64c3c3](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/e64c3c3475d4ad10e3acc6765ad2f3703de93160))

## [0.1.5](https://github.com/thenvoi/phoenix-channels-python-client-alpha/compare/phoenix-channels-python-client-alpha-v0.1.4...phoenix-channels-python-client-alpha-v0.1.5) (2026-02-25)


### Bug Fixes

* **deps:** bump requests from 2.32.4 to 2.32.5 in /.github/actions/GithubToken ([#25](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/25)) ([6f097b3](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/6f097b31fcba19ed916ef109b7193f0fbdfd7ebb))
* **deps:** bump requests in /.github/actions/GithubToken ([6f097b3](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/6f097b31fcba19ed916ef109b7193f0fbdfd7ebb))


### Continuous Integration

* add dependabot config and unhide all changelog sections ([#23](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/23)) ([4e6ac31](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/4e6ac3135380255d39d76bab6f501c8d1b39bfbd))
* add dependabot config with conventional commit prefixes and unhide all changelog sections ([4e6ac31](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/4e6ac3135380255d39d76bab6f501c8d1b39bfbd))
* **deps:** bump actions/checkout from 4 to 6 ([#28](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/28)) ([d0dde26](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/d0dde266f7a7cce78f422c3dda49739adcdbf601))
* **deps:** bump actions/setup-python from 5 to 6 ([#27](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/27)) ([97b6079](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/97b6079f9874eed8e6431a1eda73cdfb83c49b0f))
* **deps:** bump amannn/action-semantic-pull-request from 5 to 6 ([#29](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/29)) ([22e6e49](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/22e6e4957a4e15e2fd607f9d83276d099290a6ad))
* **deps:** bump astral-sh/setup-uv from 4 to 7 ([#26](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/26)) ([72051f6](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/72051f6ec63df3fdd617b1325da9e41da5d861e5))
* switch PyPI publishing to trusted publisher and add badge ([#22](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/22)) ([37d0ba5](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/37d0ba558ecc82c4871c6b8bc9fb6757ce10db48))

## [0.1.4](https://github.com/thenvoi/phoenix-channels-python-client-alpha/compare/phoenix-channels-python-client-alpha-v0.1.3...phoenix-channels-python-client-alpha-v0.1.4) (2026-02-25)


### Bug Fixes

* add PyPI version badge to README ([#18](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/18)) ([facf9d0](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/facf9d0ec77760648f329f51c3b3235225d1e756))


### Documentation

* add PyPI version badge to README ([facf9d0](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/facf9d0ec77760648f329f51c3b3235225d1e756))

## [0.1.3](https://github.com/thenvoi/phoenix-channels-python-client-alpha/compare/phoenix-channels-python-client-alpha-v0.1.2...phoenix-channels-python-client-alpha-v0.1.3) (2026-02-25)


### Features

* add Phoenix heartbeat to keep WebSocket connections alive ([#14](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/14)) ([5491eba](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/5491ebaf961c3add0fb1c426dc994c6ff11e0d50))


### Bug Fixes

* **ci:** skip PR title validation for dependabot ([6f18108](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/6f181083b54555b6b05fd0cf08b34834296df48c))
* **ci:** skip PR title validation for dependabot and add ci scope ([6187a9b](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/6187a9bfab1e1fd32c5ca5000541f6b766cbdd68))


### Documentation

* add naming conventions and PR title validation ([b36b997](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/b36b9972913123ade3194a6ffd7c29e8326cb85c))
* add naming conventions and PR title validation ([58fb095](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/58fb0953cbab61c38ec7adbe833b4eb3820edee3))
* Add shared Claude rules via git submodule ([#10](https://github.com/thenvoi/phoenix-channels-python-client-alpha/issues/10)) ([9a3221a](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/9a3221ab2a2bf3486166f9c1f0126327543bf724))

## [0.1.2](https://github.com/thenvoi/phoenix-channels-python-client-alpha/compare/phoenix-channels-python-client-alpha-v0.1.1...phoenix-channels-python-client-alpha-v0.1.2) (2026-01-12)


### Documentation

* add CONTRIBUTING.md guide ([312c759](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/312c759c551b54b2259284f96d4bf090dfa61d5d))
* add CONTRIBUTING.md guide ([ebbb752](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/ebbb752e8e40b51f0d1a807c43b42140043e6404))

## [0.1.1](https://github.com/thenvoi/phoenix-channels-python-client-alpha/compare/phoenix-channels-python-client-alpha-v0.1.0...phoenix-channels-python-client-alpha-v0.1.1) (2026-01-08)


### Features

* **ci:** add changelog generation with semantic versioning ([242c3f9](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/242c3f9caabec20e5aabd9d20af0715c5bfc6e5c))


### Bug Fixes

* **ci:** add local GithubToken action for public repo compatibility ([d32f77c](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/d32f77cc8c1bcb8ea6df9c3ac165c9f7933b84dc))
* **ci:** move checkout before token generation ([9dcc909](https://github.com/thenvoi/phoenix-channels-python-client-alpha/commit/9dcc90941fa647c3fe0f98fe62fe8da6350ab6e9))

## [Unreleased]

## [0.1.0] - 2024-01-01

### Added

- Initial Phoenix Channels Python client implementation
- PHXChannelsClient for WebSocket connections to Phoenix Channels
- PHXProtocolHandler for Phoenix protocol handling
- Support for Phoenix Channels protocol versions
- Logging utilities with setup_logging
- Comprehensive test suite with pytest

[Unreleased]: https://github.com/thenvoi/phoenix-channels-python-client-alpha/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/thenvoi/phoenix-channels-python-client-alpha/releases/tag/v0.1.0
