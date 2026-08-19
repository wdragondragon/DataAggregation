# AGENTS.md — DataAggregation（含 [数据实验室]）

> **仓库身份**: DataAggregation 主仓库（**jdk17 分支**，默认分支，1.0_jdk17-SNAPSHOT）
> **最后更新**: 2026-08-19

---

## 1. 项目范围

DataAggregation = **DataX 风格数据集成框架**（commons / core / job-plugins / file-transfer / data-source-plugins / plugins-loader-center / data-mock）+ **可视化管理端 data-aggregation-studio**（submodule）+ **[数据实验室] 功能模块**（在 studio submodule 内）。

不构建、不跑 `mvn install / package`、不跑测试；不主动 `git push` 到 GitHub；不运行 `kubectl/docker run` 部署命令除非用户显式要求。

---

## 2. 仓库结构（jdk17 分支）

```
/home/admin/AiCoding/DataAggregation/
├── .git/  .gitignore  .gitmodules     # submodule 已配置
├── AGENTS.md                          # ← 本文件
├── pom.xml                            # 1.0_jdk17-SNAPSHOT（Java 17）
├── README.md                          # 含 [数据实验室] 章节
├── DataAggregation适配方案.md          # gateway + studio 统一认证（参考）
│
├── commons/  core/  plugins-loader-center/    # ETL 核心 7 模块
├── data-source-plugins/  job-plugins/  file-transfer/  data-mock/
│
├── data-aggregation-skills/            # 🆕 submodule（init only，Claude Skills 文档）
│
└── data-aggregation-studio/            # 🆕 submodule @ jdk17 最新
    ├── .agents/skills/studio-design-code-standards-zh/SKILL.md  # 强制遵守
    ├── docs/studio-design-remediation-log.md      # 数据实验室维护
    ├── docs/studio-design-remediation-summary.md  # 阶段收口
    └── backend/
        ├── studio-commons/  studio-dto/  studio-core/
        ├── studio-nacos-compat-core/  studio-infra/  studio-flink/  # studio-flink 不引入 data-lab
        ├── studio-server/  studio-worker/  studio-desktop-runtime/
        ├── studio-test/                            # 24+ 现有回归测试
        │
        ├── studio-data-lab/                         # 🆕 数据实验室业务
        ├── studio-lab-worker/                      # 🆕 Agent（双模式：Docker + K8s）
        └── studio-data-lab-test/                   # 🆕 数据实验室回归测试
```

---

## 3. 关键约定（必须遵守）

### 3.1 分支与版本

| 项 | 锁定值 |
|----|--------|
| 分支 | **jdk17**（默认；不要切到 master/hj-pro-v1/consistency/fusion）|
| Java | **17** |
| Spring Boot | **3.5.13** |
| Spring Cloud | **2025.0.2** |
| Spring Cloud Alibaba | **2025.0.0.0** |
| Aggregation 版本 | **1.0_jdk17-SNAPSHOT**（studio 依赖）|
| Lombok | 1.18.30 |
| MyBatis Plus | 3.5.15（spring-boot3 starter）|

### 3.2 Submodule 约定

- `data-aggregation-studio` 和 `data-aggregation-skills` 已在 `.gitmodules` 配置
- **修改 submodule 内文件** → 在 submodule 目录内编辑、commit、push
- **修改 submodule 指针** → 在 DataAggregation 主仓库 commit submodule 引用变更
- **首次 clone** → `git submodule update --init --recursive`
- **更新到 studio 最新** → `cd data-aggregation-studio && git checkout jdk17 && git pull`，然后在 DataAggregation 主仓库 commit 指针

### 3.3 studio 编码规范（强制遵守 SKILL.md）

参考：`data-aggregation-studio/.agents/skills/studio-design-code-standards-zh/SKILL.md`

| 原则 | 实施 |
|------|------|
| 保持公开契约稳定 | 9 Service 字段对齐 baidu1.md，**不可改** |
| facade + 内部 helper/strategy | LabXxxServiceImpl + scheduler/workerclient/docker/k8s helper |
| 不新增 `return null;` | 用 `Optional` / 空集合 / `Result` |
| 不新增 `catch ignored` | 用语义变量名 + 注释说明降级 |
| schema 同步 | Java bootstrap 是事实源；schema 改动需同步 MySQL/SQLite/升级 SQL/前端/SchemaDriftRegressionTest |
| 不用 Liquibase | 沿用 Flyway |
| 长任务记录 | 写 `studio-design-remediation-log.md`（每子步骤）|
| 阶段收口 | 写 `studio-design-remediation-summary.md` |

### 3.4 [数据实验室] 模块约束

| 项 | 约束 |
|----|------|
| studio-data-lab | Java 17 + Spring Boot 3.5.13；**作为 studio-server Maven 模块**（不独立 Spring Boot）|
| studio-lab-worker | Java 17 + Spring Boot 3.5.13；**独立 Spring Boot**（部署到目标服务器）|
| lab-worker 模式 | **Docker 当前 + K8s 预留**（`LabExecutor` SPI 解耦）|
| 任务粒度 | **粗粒度**（IDE 实例生命周期 = 1 任务）|
| 通信协议 | Nacos + HTTP polling + JWT 5min 短期签名 |
| 数据库 | **独立 MySQL `data_aggregation_studio_lab`**（与 studio 库隔离，Flyway 独立）|
| 鉴权共享 | studio JWT 与 lab-worker JWT 共享同一 signing key |
| 镜像 flavor | base / data-science / pytorch / codeserver |
| 实例运行 | Phase 1 Docker 容器（ECS）；Phase 2 K8s Pod |
| 持久化 | Phase 1 Docker named volume；Phase 2 JuiceFS CSI |
| 入口 | Phase 1 Traefik Docker labels；Phase 2 IngressRoute |
| 集成方案 | `AiCoding/docs/数据实验室整合方案.md`（迁移期副本）|

### 3.5 测试约定

| 层 | 类型 | 命名 | 模块位置 | 状态 |
|----|------|------|---------|------|
| L1 | 单元 | `*Test.java` | 主模块 `src/test/` | **必做** |
| L2 | 跨模块回归 | `*RegressionTest.java` | `studio-data-lab-test/` | **必做** |
| L3 | 集成（本地） | `*IT.java` | `studio-data-lab-test/` | 按需 |
| L4 | 集成（ECS） | `LabWorkerEcs*IT.java` | `studio-data-lab-test/ecs/` | 按需 |
| L5 | E2E（ECS） | `*EcsE2ETest.java` | `studio-data-lab-test/e2e/` | 按需 |
| L6 | 性能 | `*BenchmarkTest.java` | `studio-data-lab-test/` | M3 |
| Phase 2 | K8s 集成 | `K8sLab*IT.java` | `studio-data-lab-test/k8s/` | 未来 |

测试命令（按 SKILL.md）：

```bash
cd data-aggregation-studio/backend

# studio-test 必须通过
mvn -pl studio-test -am "-Dsurefire.failIfNoSpecifiedTests=false" test

# 数据实验室新增测试
mvn -pl studio-data-lab-test -am "-Dsurefire.failIfNoSpecifiedTests=false" test

# 数据实验室 + lab-worker 单测
mvn -pl studio-data-lab,studio-lab-worker \
    "-Dsurefire.failIfNoSpecifiedTests=false" test

# DataAggregation 顶层 ETL 模块
cd ../..
mvn test
```

测试开关：

```bash
# L4 ECS 集成
mvn test -Dtest=*EcsIT -Dlab.ecs.tests=true \
  -Dlab.test.ecs.worker-url=http://ECS_IP:PORT \
  -Dlab.test.ecs.docker.host=tcp://ECS_IP:2375

# L5 ECS E2E
mvn test -Dtest=*E2ETest -Dlab.ecs.e2e=true
```

### 3.6 本地 Git 提交约定

- **全本地提交**（不 `git push` 到 GitHub）
- **原子提交**：每个 Task 完成一个 commit
- **Commit message 格式**：`<type>(<scope>): <subject>` 或 `[T-A-D1-01] <description>`
  - 例：`[T-A-D1-01] create studio-data-lab scaffold with 9 interfaces`
- **类型前缀**：`[P0]` Phase 0.0、`[T-A-D1-01]` bdr1_tasks 任务、`[T-A-LW-01]` lab-worker 任务、`[T-A-DL-01]` scheduler 任务、`[archive]` 历史归档
- **写 `studio-design-remediation-log.md`**：每个长任务记录目标/范围/兼容/验证/风险/下一步

### 3.7 密钥与敏感信息

- `docs/ECS.txt` 含明文 ECS root 密码 → **不要 echo 内容**、**不要 commit 到 DataAggregation**
- 部署相关敏感配置 → 通过环境变量或 Nacos Config 分发，不写代码
- JWT signing key → 通过 Nacos Config + 定期轮换

---

## 4. 子任务引用

### 4.1 bdr1_tasks.md 任务落位

| 任务 ID | 落位 |
|---------|------|
| T-A-D1-01..08 | studio-data-lab (SpringBoot 脚手架 + 9 接口契约 + DTO) |
| T-A-D2-01..04 | studio-data-lab (18 实现类填充逻辑) |
| T-A-D3-01..04 | studio-data-lab (SelfNotebook 业务) + studio-lab-worker (K8s/Docker 执行) |
| T-A-D4-01..04 | 跨模块端到端最小 |
| T-A-D5-01..04 | studio-lab-worker (Docker Compose 模板) |
| T-A-D6/7 (8 Service) | studio-data-lab + studio-lab-worker |
| T-A-D8-D15 | 跨模块联调/监控/压测/灰度 |
| T-A-LW-01..15 | studio-lab-worker (lab-worker 专项) |
| T-A-DL-01..08 | studio-data-lab (scheduler + worker-client) |
| T-B-D1..22 | data-aggregation-studio/frontend/apps/web/src/views/数据实验室/ |
| T-C-D1..12 | 外部 AI 平台仓库（仅契约对齐）|
| T-D-W1..3 | studio-data-lab/docs/联调/ |
| **Phase 2** T-A-LW-K8S-01..05 | studio-lab-worker K8sExecutor 完整实现 |
| **Phase 2** T-A-DL-K8S-01..03 | studio-data-lab 多 K8s 集群调度 |

### 4.2 设计文档结构

```
studio-data-lab/docs/
├── 接口契约.md
├── 字段对齐_baidu1.md
├── 设计/
│   ├── 架构总览.md
│   ├── 与_baidu_差异分析.md
│   └── lab-worker通信协议.md
├── 部署/
│   └── ECS-部署指南.md
├── 联调/
├── 压测/
├── 灰度/
└── 前端开发指南.md
```

---

## 5. 常用命令速查

```bash
# 查看 jdk17 分支状态
git -C /home/admin/AiCoding/DataAggregation branch -a
git -C /home/admin/AiCoding/DataAggregation log --oneline -10

# 首次 clone 含 submodule
git clone https://github.com/wdragondragon/DataAggregation.git
cd DataAggregation
git checkout jdk17
git submodule update --init --recursive

# 更新 studio submodule 到 jdk17 最新
cd data-aggregation-studio
git checkout jdk17
git pull
cd ..
git add data-aggregation-studio
git commit -m "[P0] bump studio submodule to jdk17 latest"

# 编译 DataAggregation 顶层 7 模块（提供 1.0_jdk17-SNAPSHOT 父 POM 给 studio 用）
cd /home/admin/AiCoding/DataAggregation
mvn -DskipTests install

# 编译 studio（含 studio-data-lab + studio-lab-worker + studio-data-lab-test）
cd data-aggregation-studio/backend
mvn -DskipTests package

# 启动 studio-server
mvn -pl studio-server -am spring-boot:run

# 启动 studio-worker
mvn -pl studio-worker -am spring-boot:run

# 打包 lab-worker
mvn -pl studio-lab-worker -am -DskipTests package

# 部署 lab-worker 到 ECS
./studio-lab-worker/scripts/deploy-ecs.sh

# 跑测试
mvn -pl studio-test,studio-data-lab-test,studio-data-lab,studio-lab-worker \
    -am "-Dsurefire.failIfNoSpecifiedTests=false" test
```

---

## 6. 安全提醒

- 不 echo `docs/ECS.txt` 内容
- 不主动 `ssh/scp` 到 ECS 除非用户显式要求
- 不 `git push` 到 GitHub 除非用户显式要求
- 不在 commit message 中包含密码、token、内部 IP
- ECS.txt 留在 `AiCoding/docs/` 原位，**不**迁入 DataAggregation

---

*文档用途: DataAggregation 主仓库约定 + [数据实验室] 功能落位指南*
*维护者: Mavis*
*最后更新: 2026-08-19*