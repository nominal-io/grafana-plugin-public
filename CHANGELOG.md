# Changelog

## [0.3.4](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.3.3...nominal-grafana-plugin@0.3.4) (2026-02-02)


### Bug Fixes

* add screenshots for Grafana Plugin catalog ([#71](https://github.com/nominal-io/grafana-plugin/issues/71)) ([808a686](https://github.com/nominal-io/grafana-plugin/commit/808a6869e6c897d4f5823f71752029be3c3699db))
* **deps:** update module github.com/grafana/grafana-plugin-sdk-go to v0.286.0 ([#69](https://github.com/nominal-io/grafana-plugin/issues/69)) ([f0a3653](https://github.com/nominal-io/grafana-plugin/commit/f0a365304908c002b347641ba5dd27201a20d424))

## [0.3.3](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.3.2...nominal-grafana-plugin@0.3.3) (2026-01-21)


### Bug Fixes

* use package-plugin outputs directly instead of manual packaging ([#67](https://github.com/nominal-io/grafana-plugin/issues/67)) ([a448569](https://github.com/nominal-io/grafana-plugin/commit/a448569817b59517690b88c480dba43070c81c97))

## [0.3.2](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.3.1...nominal-grafana-plugin@0.3.2) (2026-01-21)


### Bug Fixes

* read plugin ID from src/plugin.json instead of dist/plugin.json ([#65](https://github.com/nominal-io/grafana-plugin/issues/65)) ([7c414a4](https://github.com/nominal-io/grafana-plugin/commit/7c414a46168f3d6b8c1cab9d061f30adb586e85c))

## [0.3.1](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.3.0...nominal-grafana-plugin@0.3.1) (2026-01-21)


### Bug Fixes

* use package-plugin and manual release for component-prefixed tags ([#63](https://github.com/nominal-io/grafana-plugin/issues/63)) ([f38c74e](https://github.com/nominal-io/grafana-plugin/commit/f38c74e4f20e84d4351bc723219e0c5f8ae8c659))

## [0.3.0](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin-v0.2.0...nominal-grafana-plugin@0.3.0) (2026-01-21)


### Features

* add plugin release workflow for easy client distribution ([#56](https://github.com/nominal-io/grafana-plugin/issues/56)) ([ae9913b](https://github.com/nominal-io/grafana-plugin/commit/ae9913b80a82e3d5feb3343448200f0f7451163b))
* add self-hosted Renovate for dependency updates ([#47](https://github.com/nominal-io/grafana-plugin/issues/47)) ([9542695](https://github.com/nominal-io/grafana-plugin/commit/95426954badb9a505f292394a18a446b99c36db1))


### Bug Fixes

* add a networkpolicy ([#28](https://github.com/nominal-io/grafana-plugin/issues/28)) ([cac6d5b](https://github.com/nominal-io/grafana-plugin/commit/cac6d5bda13267fe63fad7b92bb1a5acbce32838))
* add package.json to release-please config and fix plugin metadata ([#62](https://github.com/nominal-io/grafana-plugin/issues/62)) ([78d10e6](https://github.com/nominal-io/grafana-plugin/commit/78d10e64ce380111f8bc31c70d8c0bc5be04cbdc))
* adjust config and ci ([#13](https://github.com/nominal-io/grafana-plugin/issues/13)) ([0b24ac7](https://github.com/nominal-io/grafana-plugin/commit/0b24ac79ff097d828181c7106e322ca472b28eec))
* clarify base url in README ([#31](https://github.com/nominal-io/grafana-plugin/issues/31)) ([510af3f](https://github.com/nominal-io/grafana-plugin/commit/510af3f147f9b9e6bb77696910065b617d16ca0f))
* **deps:** update go dependencies (non-major) ([#50](https://github.com/nominal-io/grafana-plugin/issues/50)) ([02b1df4](https://github.com/nominal-io/grafana-plugin/commit/02b1df4be8b4eb8fe000e62bf9faba0c403ffa09))
* **deps:** update module github.com/grafana/grafana-plugin-sdk-go to v0.285.0 ([#55](https://github.com/nominal-io/grafana-plugin/issues/55)) ([21978f4](https://github.com/nominal-io/grafana-plugin/commit/21978f47acda497f2ae36d728a074cacdae19973))
* docker build process with authentication and documentation cleanup ([#12](https://github.com/nominal-io/grafana-plugin/issues/12)) ([33585a6](https://github.com/nominal-io/grafana-plugin/commit/33585a60505aebfff3546f79214504beccaa3a84))
* docker image build in CI ([#17](https://github.com/nominal-io/grafana-plugin/issues/17)) ([6520576](https://github.com/nominal-io/grafana-plugin/commit/65205766dd72132f12832077c99d56981b0a29d2))
* lint and packaging warnings in docker and grafana UI ([#20](https://github.com/nominal-io/grafana-plugin/issues/20)) ([e4a9f3e](https://github.com/nominal-io/grafana-plugin/commit/e4a9f3e8ed658c02fd4685c19cdc31bfef05ab4b))
* modify docker build for images ([#16](https://github.com/nominal-io/grafana-plugin/issues/16)) ([1aa026e](https://github.com/nominal-io/grafana-plugin/commit/1aa026ed0fcf718ef859faab8a34e0c1b1e817bb))
* remove archive and cleanup cruft ([#35](https://github.com/nominal-io/grafana-plugin/issues/35)) ([370fd0e](https://github.com/nominal-io/grafana-plugin/commit/370fd0e1b8bbb7f39e7e87f3865c77dc1c94af94))
* remove datasource setup ([#29](https://github.com/nominal-io/grafana-plugin/issues/29)) ([a967192](https://github.com/nominal-io/grafana-plugin/commit/a9671920a82407ddd34d25421af7ae1ea7410872))
* revert updating to vendored package and latest api ([#44](https://github.com/nominal-io/grafana-plugin/issues/44)) ([00c23ed](https://github.com/nominal-io/grafana-plugin/commit/00c23edf05f4b3fd3e5b159dc1d93d36036aec4d))
* small issue on pathing for release please config ([#14](https://github.com/nominal-io/grafana-plugin/issues/14)) ([1678b1e](https://github.com/nominal-io/grafana-plugin/commit/1678b1e022913bcaddcc00e6c71d6721657250eb))
* update release workflow for release-please tag format ([#60](https://github.com/nominal-io/grafana-plugin/issues/60)) ([127662e](https://github.com/nominal-io/grafana-plugin/commit/127662eb66d071d22ebcceb1b93c79b50522c202))
* update runner ([#19](https://github.com/nominal-io/grafana-plugin/issues/19)) ([8da57f5](https://github.com/nominal-io/grafana-plugin/commit/8da57f5fb9d979767fa7e45e15f979eacb8c67cc))


### Chores

* bump grafana versions ([#38](https://github.com/nominal-io/grafana-plugin/issues/38)) ([72111df](https://github.com/nominal-io/grafana-plugin/commit/72111df8abc4b173e7c79a5a160fed0507493cbb))
* release main ([#15](https://github.com/nominal-io/grafana-plugin/issues/15)) ([3469ab8](https://github.com/nominal-io/grafana-plugin/commit/3469ab8df2a1c5b5efe0c6fa8821679a51baf0dd))
* release main ([#18](https://github.com/nominal-io/grafana-plugin/issues/18)) ([6e3b128](https://github.com/nominal-io/grafana-plugin/commit/6e3b128befc2ce792668fbd271f656115f956023))
* release main ([#21](https://github.com/nominal-io/grafana-plugin/issues/21)) ([cc1ce58](https://github.com/nominal-io/grafana-plugin/commit/cc1ce58c99a9997e678c810fbf0c994cf081aa49))
* release main ([#23](https://github.com/nominal-io/grafana-plugin/issues/23)) ([af29f4d](https://github.com/nominal-io/grafana-plugin/commit/af29f4d0ece10ba00e708e23af16419014b2d498))
* release main ([#25](https://github.com/nominal-io/grafana-plugin/issues/25)) ([f90bf60](https://github.com/nominal-io/grafana-plugin/commit/f90bf604d2f3cfb1cf6d3783478d342787301752))
* release main ([#26](https://github.com/nominal-io/grafana-plugin/issues/26)) ([9926f7e](https://github.com/nominal-io/grafana-plugin/commit/9926f7e0eca188fe62471882b2df6c8608a2b7d6))
* release main ([#30](https://github.com/nominal-io/grafana-plugin/issues/30)) ([220130a](https://github.com/nominal-io/grafana-plugin/commit/220130a6618c5ee75e1ed510e1b49800fa2ec070))
* release main ([#32](https://github.com/nominal-io/grafana-plugin/issues/32)) ([4e12914](https://github.com/nominal-io/grafana-plugin/commit/4e129140096613c550032ba63f99ef9a47cbac00))
* release main ([#34](https://github.com/nominal-io/grafana-plugin/issues/34)) ([86b28db](https://github.com/nominal-io/grafana-plugin/commit/86b28db898df8d6f378e89116d42c3fefa99470c))
* release main ([#36](https://github.com/nominal-io/grafana-plugin/issues/36)) ([db9f30e](https://github.com/nominal-io/grafana-plugin/commit/db9f30e9c64ff9a5bb05a9d5cd1031c2509c439e))
* release main ([#39](https://github.com/nominal-io/grafana-plugin/issues/39)) ([94bf90f](https://github.com/nominal-io/grafana-plugin/commit/94bf90ff860e29f458e982402b4807fab7478448))
* release main ([#42](https://github.com/nominal-io/grafana-plugin/issues/42)) ([4a5fc25](https://github.com/nominal-io/grafana-plugin/commit/4a5fc25a245a5d03c395ad572cc5b372d7c36111))
* release main ([#45](https://github.com/nominal-io/grafana-plugin/issues/45)) ([ad44182](https://github.com/nominal-io/grafana-plugin/commit/ad44182fb7188a8ffde205872476e15c066fb1a3))
* release main ([#48](https://github.com/nominal-io/grafana-plugin/issues/48)) ([7a1aa2f](https://github.com/nominal-io/grafana-plugin/commit/7a1aa2f5e10c7d436551198c4af3fe65cde1c655))
* release main ([#59](https://github.com/nominal-io/grafana-plugin/issues/59)) ([a640525](https://github.com/nominal-io/grafana-plugin/commit/a640525481fd0420882d43ba615028259148cfd1))
* setup secret for plugin ([#37](https://github.com/nominal-io/grafana-plugin/issues/37)) ([42336a5](https://github.com/nominal-io/grafana-plugin/commit/42336a526f89c5fd623a418d5eeeb16f93b82895))
* updating to vendored package and latest api ([#40](https://github.com/nominal-io/grafana-plugin/issues/40)) ([d9ff9f1](https://github.com/nominal-io/grafana-plugin/commit/d9ff9f194b8806f684d3ce1a64f605e9782fb8f9))

## [0.2.0](https://github.com/nominal-io/grafana-plugin/compare/0.1.0...0.2.0) (2026-01-21)


### Features

* add plugin release workflow for easy client distribution ([#56](https://github.com/nominal-io/grafana-plugin/issues/56)) ([ae9913b](https://github.com/nominal-io/grafana-plugin/commit/ae9913b80a82e3d5feb3343448200f0f7451163b))
* add self-hosted Renovate for dependency updates ([#47](https://github.com/nominal-io/grafana-plugin/issues/47)) ([9542695](https://github.com/nominal-io/grafana-plugin/commit/95426954badb9a505f292394a18a446b99c36db1))


### Bug Fixes

* add a networkpolicy ([#28](https://github.com/nominal-io/grafana-plugin/issues/28)) ([cac6d5b](https://github.com/nominal-io/grafana-plugin/commit/cac6d5bda13267fe63fad7b92bb1a5acbce32838))
* adjust config and ci ([#13](https://github.com/nominal-io/grafana-plugin/issues/13)) ([0b24ac7](https://github.com/nominal-io/grafana-plugin/commit/0b24ac79ff097d828181c7106e322ca472b28eec))
* clarify base url in README ([#31](https://github.com/nominal-io/grafana-plugin/issues/31)) ([510af3f](https://github.com/nominal-io/grafana-plugin/commit/510af3f147f9b9e6bb77696910065b617d16ca0f))
* **deps:** update go dependencies (non-major) ([#50](https://github.com/nominal-io/grafana-plugin/issues/50)) ([02b1df4](https://github.com/nominal-io/grafana-plugin/commit/02b1df4be8b4eb8fe000e62bf9faba0c403ffa09))
* **deps:** update module github.com/grafana/grafana-plugin-sdk-go to v0.285.0 ([#55](https://github.com/nominal-io/grafana-plugin/issues/55)) ([21978f4](https://github.com/nominal-io/grafana-plugin/commit/21978f47acda497f2ae36d728a074cacdae19973))
* docker build process with authentication and documentation cleanup ([#12](https://github.com/nominal-io/grafana-plugin/issues/12)) ([33585a6](https://github.com/nominal-io/grafana-plugin/commit/33585a60505aebfff3546f79214504beccaa3a84))
* docker image build in CI ([#17](https://github.com/nominal-io/grafana-plugin/issues/17)) ([6520576](https://github.com/nominal-io/grafana-plugin/commit/65205766dd72132f12832077c99d56981b0a29d2))
* lint and packaging warnings in docker and grafana UI ([#20](https://github.com/nominal-io/grafana-plugin/issues/20)) ([e4a9f3e](https://github.com/nominal-io/grafana-plugin/commit/e4a9f3e8ed658c02fd4685c19cdc31bfef05ab4b))
* modify docker build for images ([#16](https://github.com/nominal-io/grafana-plugin/issues/16)) ([1aa026e](https://github.com/nominal-io/grafana-plugin/commit/1aa026ed0fcf718ef859faab8a34e0c1b1e817bb))
* remove archive and cleanup cruft ([#35](https://github.com/nominal-io/grafana-plugin/issues/35)) ([370fd0e](https://github.com/nominal-io/grafana-plugin/commit/370fd0e1b8bbb7f39e7e87f3865c77dc1c94af94))
* remove datasource setup ([#29](https://github.com/nominal-io/grafana-plugin/issues/29)) ([a967192](https://github.com/nominal-io/grafana-plugin/commit/a9671920a82407ddd34d25421af7ae1ea7410872))
* revert updating to vendored package and latest api ([#44](https://github.com/nominal-io/grafana-plugin/issues/44)) ([00c23ed](https://github.com/nominal-io/grafana-plugin/commit/00c23edf05f4b3fd3e5b159dc1d93d36036aec4d))
* small issue on pathing for release please config ([#14](https://github.com/nominal-io/grafana-plugin/issues/14)) ([1678b1e](https://github.com/nominal-io/grafana-plugin/commit/1678b1e022913bcaddcc00e6c71d6721657250eb))
* update runner ([#19](https://github.com/nominal-io/grafana-plugin/issues/19)) ([8da57f5](https://github.com/nominal-io/grafana-plugin/commit/8da57f5fb9d979767fa7e45e15f979eacb8c67cc))


### Chores

* bump grafana versions ([#38](https://github.com/nominal-io/grafana-plugin/issues/38)) ([72111df](https://github.com/nominal-io/grafana-plugin/commit/72111df8abc4b173e7c79a5a160fed0507493cbb))
* release main ([#15](https://github.com/nominal-io/grafana-plugin/issues/15)) ([3469ab8](https://github.com/nominal-io/grafana-plugin/commit/3469ab8df2a1c5b5efe0c6fa8821679a51baf0dd))
* release main ([#18](https://github.com/nominal-io/grafana-plugin/issues/18)) ([6e3b128](https://github.com/nominal-io/grafana-plugin/commit/6e3b128befc2ce792668fbd271f656115f956023))
* release main ([#21](https://github.com/nominal-io/grafana-plugin/issues/21)) ([cc1ce58](https://github.com/nominal-io/grafana-plugin/commit/cc1ce58c99a9997e678c810fbf0c994cf081aa49))
* release main ([#23](https://github.com/nominal-io/grafana-plugin/issues/23)) ([af29f4d](https://github.com/nominal-io/grafana-plugin/commit/af29f4d0ece10ba00e708e23af16419014b2d498))
* release main ([#25](https://github.com/nominal-io/grafana-plugin/issues/25)) ([f90bf60](https://github.com/nominal-io/grafana-plugin/commit/f90bf604d2f3cfb1cf6d3783478d342787301752))
* release main ([#26](https://github.com/nominal-io/grafana-plugin/issues/26)) ([9926f7e](https://github.com/nominal-io/grafana-plugin/commit/9926f7e0eca188fe62471882b2df6c8608a2b7d6))
* release main ([#30](https://github.com/nominal-io/grafana-plugin/issues/30)) ([220130a](https://github.com/nominal-io/grafana-plugin/commit/220130a6618c5ee75e1ed510e1b49800fa2ec070))
* release main ([#32](https://github.com/nominal-io/grafana-plugin/issues/32)) ([4e12914](https://github.com/nominal-io/grafana-plugin/commit/4e129140096613c550032ba63f99ef9a47cbac00))
* release main ([#34](https://github.com/nominal-io/grafana-plugin/issues/34)) ([86b28db](https://github.com/nominal-io/grafana-plugin/commit/86b28db898df8d6f378e89116d42c3fefa99470c))
* release main ([#36](https://github.com/nominal-io/grafana-plugin/issues/36)) ([db9f30e](https://github.com/nominal-io/grafana-plugin/commit/db9f30e9c64ff9a5bb05a9d5cd1031c2509c439e))
* release main ([#39](https://github.com/nominal-io/grafana-plugin/issues/39)) ([94bf90f](https://github.com/nominal-io/grafana-plugin/commit/94bf90ff860e29f458e982402b4807fab7478448))
* release main ([#42](https://github.com/nominal-io/grafana-plugin/issues/42)) ([4a5fc25](https://github.com/nominal-io/grafana-plugin/commit/4a5fc25a245a5d03c395ad572cc5b372d7c36111))
* release main ([#45](https://github.com/nominal-io/grafana-plugin/issues/45)) ([ad44182](https://github.com/nominal-io/grafana-plugin/commit/ad44182fb7188a8ffde205872476e15c066fb1a3))
* release main ([#48](https://github.com/nominal-io/grafana-plugin/issues/48)) ([7a1aa2f](https://github.com/nominal-io/grafana-plugin/commit/7a1aa2f5e10c7d436551198c4af3fe65cde1c655))
* setup secret for plugin ([#37](https://github.com/nominal-io/grafana-plugin/issues/37)) ([42336a5](https://github.com/nominal-io/grafana-plugin/commit/42336a526f89c5fd623a418d5eeeb16f93b82895))
* updating to vendored package and latest api ([#40](https://github.com/nominal-io/grafana-plugin/issues/40)) ([d9ff9f1](https://github.com/nominal-io/grafana-plugin/commit/d9ff9f194b8806f684d3ce1a64f605e9782fb8f9))

## [0.1.0](https://github.com/nominal-io/grafana-plugin/compare/v0.0.13...0.1.0) (2026-01-21)


### Features

* add plugin release workflow for easy client distribution ([#56](https://github.com/nominal-io/grafana-plugin/issues/56)) ([ae9913b](https://github.com/nominal-io/grafana-plugin/commit/ae9913b80a82e3d5feb3343448200f0f7451163b))
* add self-hosted Renovate for dependency updates ([#47](https://github.com/nominal-io/grafana-plugin/issues/47)) ([9542695](https://github.com/nominal-io/grafana-plugin/commit/95426954badb9a505f292394a18a446b99c36db1))


### Bug Fixes

* add a networkpolicy ([#28](https://github.com/nominal-io/grafana-plugin/issues/28)) ([cac6d5b](https://github.com/nominal-io/grafana-plugin/commit/cac6d5bda13267fe63fad7b92bb1a5acbce32838))
* adjust config and ci ([#13](https://github.com/nominal-io/grafana-plugin/issues/13)) ([0b24ac7](https://github.com/nominal-io/grafana-plugin/commit/0b24ac79ff097d828181c7106e322ca472b28eec))
* clarify base url in README ([#31](https://github.com/nominal-io/grafana-plugin/issues/31)) ([510af3f](https://github.com/nominal-io/grafana-plugin/commit/510af3f147f9b9e6bb77696910065b617d16ca0f))
* **deps:** update go dependencies (non-major) ([#50](https://github.com/nominal-io/grafana-plugin/issues/50)) ([02b1df4](https://github.com/nominal-io/grafana-plugin/commit/02b1df4be8b4eb8fe000e62bf9faba0c403ffa09))
* **deps:** update module github.com/grafana/grafana-plugin-sdk-go to v0.285.0 ([#55](https://github.com/nominal-io/grafana-plugin/issues/55)) ([21978f4](https://github.com/nominal-io/grafana-plugin/commit/21978f47acda497f2ae36d728a074cacdae19973))
* docker build process with authentication and documentation cleanup ([#12](https://github.com/nominal-io/grafana-plugin/issues/12)) ([33585a6](https://github.com/nominal-io/grafana-plugin/commit/33585a60505aebfff3546f79214504beccaa3a84))
* docker image build in CI ([#17](https://github.com/nominal-io/grafana-plugin/issues/17)) ([6520576](https://github.com/nominal-io/grafana-plugin/commit/65205766dd72132f12832077c99d56981b0a29d2))
* lint and packaging warnings in docker and grafana UI ([#20](https://github.com/nominal-io/grafana-plugin/issues/20)) ([e4a9f3e](https://github.com/nominal-io/grafana-plugin/commit/e4a9f3e8ed658c02fd4685c19cdc31bfef05ab4b))
* modify docker build for images ([#16](https://github.com/nominal-io/grafana-plugin/issues/16)) ([1aa026e](https://github.com/nominal-io/grafana-plugin/commit/1aa026ed0fcf718ef859faab8a34e0c1b1e817bb))
* remove archive and cleanup cruft ([#35](https://github.com/nominal-io/grafana-plugin/issues/35)) ([370fd0e](https://github.com/nominal-io/grafana-plugin/commit/370fd0e1b8bbb7f39e7e87f3865c77dc1c94af94))
* remove datasource setup ([#29](https://github.com/nominal-io/grafana-plugin/issues/29)) ([a967192](https://github.com/nominal-io/grafana-plugin/commit/a9671920a82407ddd34d25421af7ae1ea7410872))
* revert updating to vendored package and latest api ([#44](https://github.com/nominal-io/grafana-plugin/issues/44)) ([00c23ed](https://github.com/nominal-io/grafana-plugin/commit/00c23edf05f4b3fd3e5b159dc1d93d36036aec4d))
* small issue on pathing for release please config ([#14](https://github.com/nominal-io/grafana-plugin/issues/14)) ([1678b1e](https://github.com/nominal-io/grafana-plugin/commit/1678b1e022913bcaddcc00e6c71d6721657250eb))
* update runner ([#19](https://github.com/nominal-io/grafana-plugin/issues/19)) ([8da57f5](https://github.com/nominal-io/grafana-plugin/commit/8da57f5fb9d979767fa7e45e15f979eacb8c67cc))


### Chores

* bump grafana versions ([#38](https://github.com/nominal-io/grafana-plugin/issues/38)) ([72111df](https://github.com/nominal-io/grafana-plugin/commit/72111df8abc4b173e7c79a5a160fed0507493cbb))
* release main ([#15](https://github.com/nominal-io/grafana-plugin/issues/15)) ([3469ab8](https://github.com/nominal-io/grafana-plugin/commit/3469ab8df2a1c5b5efe0c6fa8821679a51baf0dd))
* release main ([#18](https://github.com/nominal-io/grafana-plugin/issues/18)) ([6e3b128](https://github.com/nominal-io/grafana-plugin/commit/6e3b128befc2ce792668fbd271f656115f956023))
* release main ([#21](https://github.com/nominal-io/grafana-plugin/issues/21)) ([cc1ce58](https://github.com/nominal-io/grafana-plugin/commit/cc1ce58c99a9997e678c810fbf0c994cf081aa49))
* release main ([#23](https://github.com/nominal-io/grafana-plugin/issues/23)) ([af29f4d](https://github.com/nominal-io/grafana-plugin/commit/af29f4d0ece10ba00e708e23af16419014b2d498))
* release main ([#25](https://github.com/nominal-io/grafana-plugin/issues/25)) ([f90bf60](https://github.com/nominal-io/grafana-plugin/commit/f90bf604d2f3cfb1cf6d3783478d342787301752))
* release main ([#26](https://github.com/nominal-io/grafana-plugin/issues/26)) ([9926f7e](https://github.com/nominal-io/grafana-plugin/commit/9926f7e0eca188fe62471882b2df6c8608a2b7d6))
* release main ([#30](https://github.com/nominal-io/grafana-plugin/issues/30)) ([220130a](https://github.com/nominal-io/grafana-plugin/commit/220130a6618c5ee75e1ed510e1b49800fa2ec070))
* release main ([#32](https://github.com/nominal-io/grafana-plugin/issues/32)) ([4e12914](https://github.com/nominal-io/grafana-plugin/commit/4e129140096613c550032ba63f99ef9a47cbac00))
* release main ([#34](https://github.com/nominal-io/grafana-plugin/issues/34)) ([86b28db](https://github.com/nominal-io/grafana-plugin/commit/86b28db898df8d6f378e89116d42c3fefa99470c))
* release main ([#36](https://github.com/nominal-io/grafana-plugin/issues/36)) ([db9f30e](https://github.com/nominal-io/grafana-plugin/commit/db9f30e9c64ff9a5bb05a9d5cd1031c2509c439e))
* release main ([#39](https://github.com/nominal-io/grafana-plugin/issues/39)) ([94bf90f](https://github.com/nominal-io/grafana-plugin/commit/94bf90ff860e29f458e982402b4807fab7478448))
* release main ([#42](https://github.com/nominal-io/grafana-plugin/issues/42)) ([4a5fc25](https://github.com/nominal-io/grafana-plugin/commit/4a5fc25a245a5d03c395ad572cc5b372d7c36111))
* release main ([#45](https://github.com/nominal-io/grafana-plugin/issues/45)) ([ad44182](https://github.com/nominal-io/grafana-plugin/commit/ad44182fb7188a8ffde205872476e15c066fb1a3))
* setup secret for plugin ([#37](https://github.com/nominal-io/grafana-plugin/issues/37)) ([42336a5](https://github.com/nominal-io/grafana-plugin/commit/42336a526f89c5fd623a418d5eeeb16f93b82895))
* updating to vendored package and latest api ([#40](https://github.com/nominal-io/grafana-plugin/issues/40)) ([d9ff9f1](https://github.com/nominal-io/grafana-plugin/commit/d9ff9f194b8806f684d3ce1a64f605e9782fb8f9))

## [0.0.13](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.0.12...nominal-grafana-plugin@0.0.13) (2026-01-05)


### Bug Fixes

* revert updating to vendored package and latest api ([#44](https://github.com/nominal-io/grafana-plugin/issues/44)) ([00c23ed](https://github.com/nominal-io/grafana-plugin/commit/00c23edf05f4b3fd3e5b159dc1d93d36036aec4d))

## [0.0.12](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.0.11...nominal-grafana-plugin@0.0.12) (2026-01-05)


### Chores

* updating to vendored package and latest api ([#40](https://github.com/nominal-io/grafana-plugin/issues/40)) ([d9ff9f1](https://github.com/nominal-io/grafana-plugin/commit/d9ff9f194b8806f684d3ce1a64f605e9782fb8f9))

## [0.0.11](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.0.10...nominal-grafana-plugin@0.0.11) (2025-11-21)


### Chores

* bump grafana versions ([#38](https://github.com/nominal-io/grafana-plugin/issues/38)) ([72111df](https://github.com/nominal-io/grafana-plugin/commit/72111df8abc4b173e7c79a5a160fed0507493cbb))

## [0.0.10](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.0.9...nominal-grafana-plugin@0.0.10) (2025-10-23)


### Bug Fixes

* remove archive and cleanup cruft ([#35](https://github.com/nominal-io/grafana-plugin/issues/35)) ([370fd0e](https://github.com/nominal-io/grafana-plugin/commit/370fd0e1b8bbb7f39e7e87f3865c77dc1c94af94))


### Chores

* setup secret for plugin ([#37](https://github.com/nominal-io/grafana-plugin/issues/37)) ([42336a5](https://github.com/nominal-io/grafana-plugin/commit/42336a526f89c5fd623a418d5eeeb16f93b82895))

## [0.0.9](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.0.8...nominal-grafana-plugin@0.0.9) (2025-09-12)


### Bug Fixes


## [0.0.8](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.0.7...nominal-grafana-plugin@0.0.8) (2025-09-12)


### Bug Fixes

* clarify base url in README ([#31](https://github.com/nominal-io/grafana-plugin/issues/31)) ([510af3f](https://github.com/nominal-io/grafana-plugin/commit/510af3f147f9b9e6bb77696910065b617d16ca0f))

## [0.0.7](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.0.6...nominal-grafana-plugin@0.0.7) (2025-09-11)


### Bug Fixes

* remove datasource setup ([#29](https://github.com/nominal-io/grafana-plugin/issues/29)) ([a967192](https://github.com/nominal-io/grafana-plugin/commit/a9671920a82407ddd34d25421af7ae1ea7410872))

## [0.0.6](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.0.5...nominal-grafana-plugin@0.0.6) (2025-09-11)


### Bug Fixes

* add a networkpolicy ([#28](https://github.com/nominal-io/grafana-plugin/issues/28)) ([cac6d5b](https://github.com/nominal-io/grafana-plugin/commit/cac6d5bda13267fe63fad7b92bb1a5acbce32838))
* lint and packaging warnings in docker and grafana UI ([#20](https://github.com/nominal-io/grafana-plugin/issues/20)) ([e4a9f3e](https://github.com/nominal-io/grafana-plugin/commit/e4a9f3e8ed658c02fd4685c19cdc31bfef05ab4b))

## [0.0.5](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.0.4...nominal-grafana-plugin@0.0.5) (2025-09-10)


### Bug Fixes


## [0.0.4](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.0.3...nominal-grafana-plugin@0.0.4) (2025-09-10)


### Bug Fixes


## [0.0.3](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.0.2...nominal-grafana-plugin@0.0.3) (2025-09-10)


### Bug Fixes

* update runner ([#19](https://github.com/nominal-io/grafana-plugin/issues/19)) ([8da57f5](https://github.com/nominal-io/grafana-plugin/commit/8da57f5fb9d979767fa7e45e15f979eacb8c67cc))

## [0.0.2](https://github.com/nominal-io/grafana-plugin/compare/nominal-grafana-plugin@0.0.1...nominal-grafana-plugin@0.0.2) (2025-09-10)


### Bug Fixes

* docker image build in CI ([#17](https://github.com/nominal-io/grafana-plugin/issues/17)) ([6520576](https://github.com/nominal-io/grafana-plugin/commit/65205766dd72132f12832077c99d56981b0a29d2))
* modify docker build for images ([#16](https://github.com/nominal-io/grafana-plugin/issues/16)) ([1aa026e](https://github.com/nominal-io/grafana-plugin/commit/1aa026ed0fcf718ef859faab8a34e0c1b1e817bb))

## 0.0.1 (2025-09-10)


### Bug Fixes

* adjust config and ci ([#13](https://github.com/nominal-io/grafana-plugin/issues/13)) ([0b24ac7](https://github.com/nominal-io/grafana-plugin/commit/0b24ac79ff097d828181c7106e322ca472b28eec))
* docker build process with authentication and documentation cleanup ([#12](https://github.com/nominal-io/grafana-plugin/issues/12)) ([33585a6](https://github.com/nominal-io/grafana-plugin/commit/33585a60505aebfff3546f79214504beccaa3a84))
* small issue on pathing for release please config ([#14](https://github.com/nominal-io/grafana-plugin/issues/14)) ([1678b1e](https://github.com/nominal-io/grafana-plugin/commit/1678b1e022913bcaddcc00e6c71d6721657250eb))
