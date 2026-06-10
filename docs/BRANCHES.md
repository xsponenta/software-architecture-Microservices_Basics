# Branch Guide

The repository is organized as a sequence of coursework branches. This keeps each lab submission reproducible without mixing infrastructure from later tasks into earlier ones.

## Branch Map

| Branch | Stage | What to inspect |
| --- | --- | --- |
| `main` | Documentation index | This README and documentation only |
| `micro_basics` | Lab 1 / baseline microservices | `facade_service/`, `logging_service/`, `messages_service/`, `proto/`, `docker-compose.yml` |
| `micro_hazelcast` | Hazelcast distributed state | Hazelcast cluster config, replicated logging service setup |
| `micro_mq` | Message queue architecture | `config_service/`, counter flow, Hazelcast Queue, async delivery |
| `micro_consul` | Consul integration | Consul service registry, KV config, health-check based discovery |
| `hazelcast` | Earlier experiment | Historical Hazelcast branch kept for comparison |

## Recommended Review Order

1. Start with `micro_basics` to see the minimal service split.
2. Move to `micro_hazelcast` for replicated logging and distributed storage.
3. Move to `micro_mq` for asynchronous counter updates.
4. Finish with `micro_consul` for service discovery, configuration, and failure tests.

## Useful Commands

Fetch every branch:

```bash
git fetch --all --prune
```

Switch to a branch:

```bash
git checkout micro_consul
```

Compare two stages:

```bash
git diff micro_mq..micro_consul --stat
```

List files in a branch without checking it out:

```bash
git ls-tree -r --name-only origin/micro_consul
```

## Why Not Merge Everything Into Main?

The branches represent different assignment checkpoints. Later branches intentionally change service topology, configuration, and runtime behavior, so merging all code into `main` would make the earlier labs harder to review. The cleaner structure is:

- `main` documents the whole project;
- each lab branch remains runnable on its own;
- branch READMEs document branch-specific commands and checks.
