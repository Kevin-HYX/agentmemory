# AgentMemory 搜索优化部署验收

日期：2026-10-11（北京时间）。目标：home-computer。

## 发布内容

生产实体检索和观测扩展在每次请求内各构建一次节点映射、邻接表，全部起点遍历复用。补丁不修改结果生成、评分、排序或存储格式。

源码工作区基线：`e04ba88819c365c9acf9d6661ea802143e728bd6`，位于 `D:\Agentmemory`，本地 main 与 origin/main 同步。工作区已有其他未提交修改，本次只新增 `deploy/search-performance-20261011/` 中的部署补丁、测试、脚本及说明；未提交、推送或合并 Git。

原镜像：`local/agentmemory:0.9.29-sharded-20260908`

原镜像 ID：`sha256:7cbd2becbf7a57a07fa304c311db36bdcef9996268f1793ad57e27510e19939c`

新镜像：`local/agentmemory:0.9.29-search-performance-20261011`

新镜像 ID：`sha256:5c447fa0881539feb8b301daa548173e1b17689b3cd9f2ae59c36d3f5a752aaf`

补丁覆盖生产 `index.mjs` 与 `src-G7yt8gGm.mjs` 中的同一图谱模块。应用版本仍为 0.9.29，通过镜像标签和 `org.agentmemory.patch` 标识此次部署。

`patch-search.mjs` SHA-256：`8220aa68b74648274aeebc085aeaf346014fa2dc4769a5d2180a761a37e1bb59`

新 `index.mjs` SHA-256：`04749f005928757274c34826ec73da7c1b1e83ad31ab7185a0a9ee3f9ca1815e`

## 部署前备份

机械硬盘：`/dev/sda1`，ext4，挂载于 `/data`。

备份目录：`/data/backups/agentmemory/20261011-search-performance/`，访问权限 0700。

在线初次复制后停止 AgentMemory 与 frpc，以 rsync 同步完整服务目录，生成并校验 5,864 个文件的 SHA-256；完成标记时间为 `2026-10-11T06:28:10Z`（北京时间 14:28:10）。包含一致数据副本、部署配置、原镜像和原部署补丁，整体约 21 GiB。

原镜像已从机械硬盘中的 `original-image.tar` 实际执行 `docker image load`，恢复出的镜像 ID 与原生产镜像一致。备份文件清单为 `service.sha256`，原镜像清单为 `original-image.sha256`，完成标记为 `CONSISTENT_BACKUP_VERIFIED`。

## 验收证据

- 旧生产代码的重复建图回归检查先失败：80 次构建，期望 1 次；补丁后通过。
- Docker 构建中的六组对照覆盖实体检索、观测扩展、无匹配、时间查询，完整结果、得分和顺序均一致；重复应用及未知源码锚点会明确失败。
- 图谱、混合搜索、搜索、智能搜索及 MCP 独立／代理测试共 6 个测试文件、86 项通过。
- 真实图谱固定快照：72,098 个节点、139,346 条边；同一 `AgentMemory` 查询的完整结果一致，建图次数 349→1，纯计算耗时 36,591→217 毫秒。此实验先读取一次同一份图谱，耗时不含图谱 I/O。
- 部署前完整 REST 搜索实测 32.67／32.79 秒；本轮还出现一次 90 秒超时。
- 部署后 REST：`AgentMemory` 3,011／2,729 毫秒；`STA4003` 2,861 毫秒；`数学知识管理体系` 1,391 毫秒；智能搜索 `AgentMemory` 2,583 毫秒；均 HTTP 200，返回 3 条记录。
- 再次重复容器内 `AgentMemory` 查询：2,866／2,762／2,658 毫秒，结果一致。
- 从当前 Windows 客户端直接访问 MCP 同一鉴权 HTTPS 搜索路径：3,089／2,802 毫秒，HTTP 200，返回 3 条记录。
- 聊天中 AgentMemory MCP 两次调用：9,217／18,981 毫秒，均正常返回。直接 HTTPS 已稳定在约 3 秒，剩余时间属于 MCP 调用链额外开销；具体位置尚未定位。
- 新容器 Docker 健康检查通过，详细健康接口 `healthy`、alerts 为空，KV 延迟 1.53 毫秒，模型熔断器关闭；重启计数 0、OOMKilled=false。补测时容器内存约 3.74 GiB / 20 GiB。

验收 JSON：`/home/kevin/services/agentmemory/search-performance-20261011/acceptance.json`，另存于机械硬盘备份目录。

## 回滚

服务器发布文件位于 `/home/kevin/services/agentmemory/search-performance-20261011/`。

回滚命令：

```bash
bash /home/kevin/services/agentmemory/search-performance-20261011/rollback.sh
```

脚本检查一致备份标记，必要时校验并加载原镜像，恢复原 Compose 配置后重建 AgentMemory。补丁无存储迁移，常规代码回滚保留当前数据，避免覆盖发布后新增的记忆。

## 已知边界

本次未发布工作区既有的图谱来源外置、存储分片变化、CLI、导出等其他修改。它们需要独立的数据迁移验收。

MCP 调用链额外延迟仍波动，不能用本次服务端约 12 倍提速声称聊天工具端到端也达到同样倍率。此前的 MiniMax 限流属于记忆加工链路，未在本次性能补丁中改变模型配置。
