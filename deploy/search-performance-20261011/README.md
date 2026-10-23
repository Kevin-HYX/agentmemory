# AgentMemory 搜索性能补丁（2026-10-11）

本补丁基于 home-computer 的 `local/agentmemory:0.9.29-sharded-20260908` 镜像。
实体检索和观测扩展分别在每次请求内构建一次节点映射与邻接表，遍历复用这份索引。
结果生成、评分、排序、分片格式及图谱来源数据保持原实现。

D 盘源码的 `src/functions/graph-retrieval.ts` 已有相同的索引复用思路，但还包含来源外置、反向索引等存储变化。本次以独立部署补丁应用已验证的性能改进，避免连带发布工作区其他未提交改动。

`patch-search.mjs` 对生产模块锚点进行严格检查，遇到未知版本或重复应用即停止。Docker 构建执行旧/新结果对照、单次索引构建检查和语法检查。

`test-search-patch.mjs <原生产 bundle>` 对比实体检索、观测扩展、无匹配和时间查询的完整结果。追加 `--expect-single` 可在旧实现上复现重复建图失败，在补丁后通过。

`verify-real-graph.mjs` 通过 iii SDK 读取生产图谱，在同一份数据上比较旧/新完整结果和速度，不写业务状态。

部署备份目录：home-computer `/data/backups/agentmemory/20261011-search-performance/`（`/dev/sda1` 机械硬盘）。其中保存原服务目录、一致数据副本、原镜像及校验清单。
发布目录：`/home/kevin/services/agentmemory/search-performance-20261011/`。
回滚使用备份中的 `service/compose.yaml` 与 `original-image.tar`。本补丁无存储迁移，常规回滚只替换镜像与 Compose 配置，无须覆盖部署之后新增的数据。
