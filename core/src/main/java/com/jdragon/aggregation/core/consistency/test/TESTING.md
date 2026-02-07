# 数据一致性对比系统测试指南

## 测试环境要求

### 1. 数据库环境
- MySQL 5.6+ 或 MariaDB 10.2+
- 测试连接信息（默认）：
  - 主机：localhost
  - 端口：3306
  - 用户名：root
  - 密码：password

### 2. 数据源插件
- mysql-source 插件必须可用
- 插件路径：`data-source-plugins/data-source-handler-mysql5`

## 测试类说明

### 1. MockDataGenerator
**位置**: `core/src/main/java/com/jdragon/aggregation/core/consistency/test/MockDataGenerator.java`

**功能**:
- 使用现有数据源插件创建测试数据库
- 生成模拟数据不一致场景
- 支持数据清理

**使用示例**:
```java
MockDataGenerator generator = new MockDataGenerator("mysql-source");
generator.generateTestDatabases("localhost", "3306", "root", "password");
```

**生成的测试数据库**:
1. `primary_db` - 主数据库（高可信度，权重1.0）
2. `backup_db` - 备份数据库（中等可信度，权重0.8）
3. `warehouse_db` - 数据仓库（低可信度，权重0.9，字段名不同）

### 2. DataConsistencyIntegrationTest
**位置**: `core/src/test/java/com/jdragon/aggregation/core/consistency/DataConsistencyIntegrationTest.java`

**功能**:
- 端到端集成测试
- 验证数据一致性检查全流程
- 测试不同冲突解决策略

**运行方式**:
```bash
# 从IDE运行main方法
# 或使用Maven
mvn test -Dtest=DataConsistencyIntegrationTest
```

## 测试数据设计

### 用户表不一致场景
| 用户ID | 主数据库 | 备份数据库 | 数据仓库 | 不一致类型 |
|--------|----------|------------|----------|------------|
| 1 | age=30 | age=31 | age=30 | 年龄不一致 |
| 2 | salary=12000 | salary=12500 | salary=12000 | 工资不一致 |
| 3 | age=35, salary=18000 | age=35, salary=18000 | age=36, salary=18200 | 年龄+工资不一致 |
| 4 | department=HR | department=HR | dept=Human Resource | 部门名称不一致 |
| 8 | age=33 | age=33 | age=NULL | 空值不一致 |

### 订单表不一致场景
| 订单号 | 主数据库 | 备份数据库 | 数据仓库 | 不一致类型 |
|--------|----------|------------|----------|------------|
| ORD-2023002 | amount=3000 | amount=3100 | amount=3000 | 金额不一致 |
| ORD-2023006 | status=SHIPPED | status=DELIVERED | status=SHIPPED | 状态不一致 |
| ORD-2023003 | amount=8000 | amount=8000 | amount=8100 | 金额小差异 |
| ORD-2023005 | amount=4500 | amount=4500 | amount=4550 | 金额小差异 |

## 运行测试步骤

### 步骤1: 准备数据库
确保MySQL服务运行，并更新测试连接信息（如果需要）：
```java
// 在DataConsistencyIntegrationTest中修改
private static final String TEST_HOST = "localhost";
private static final String TEST_PORT = "3306";
private static final String TEST_USERNAME = "root";
private static final String TEST_PASSWORD = "your_password";
```

### 步骤2: 运行集成测试
```java
// 方式1: 运行DataConsistencyIntegrationTest.main()方法
// 方式2: 运行MockDataGenerator.main()生成测试数据，然后手动测试
```

### 步骤3: 验证结果
测试完成后检查：
1. 控制台输出：显示测试结果摘要
2. 生成的报告文件：`./test-results/` 目录下的JSON和HTML文件
3. 数据库验证：使用SQL客户端验证三个测试数据库的数据

## 测试覆盖率

### 核心功能测试
- [x] 数据源插件加载和管理
- [x] 多数据源数据获取
- [x] 数据分组和匹配键处理
- [x] 字段映射支持
- [x] 数据差异检测
- [x] 容错阈值处理
- [x] 冲突解决策略执行
- [x] 结果记录和报告生成

### 冲突解决策略测试
- [x] 高可信度源策略
- [x] 加权平均值策略（数值字段）
- [x] 多数投票策略
- [ ] 自定义规则策略（需实现自定义解析器）
- [ ] 人工审核策略（标记需人工处理）

## 故障排除

### 常见问题

#### 1. MySQL连接失败
```
错误: MySQL连接测试失败: Communications link failure
```
**解决方案**:
- 检查MySQL服务状态
- 验证连接参数（主机、端口、用户名、密码）
- 检查防火墙设置
- 确认用户有创建数据库的权限

#### 2. 插件加载失败
```
错误: Failed to load data source plugin: mysql-source
```
**解决方案**:
- 确认`mysql-source`插件已正确构建
- 检查插件配置文件
- 验证插件路径设置

#### 3. 字段映射错误
```
错误: Column 'department' not found
```
**解决方案**:
- 检查数据仓库表的字段名（使用`dept`而不是`department`）
- 验证字段映射配置
- 更新查询SQL中的字段别名

#### 4. 权限不足
```
错误: Access denied for user 'root'@'localhost' to database 'primary_db'
```
**解决方案**:
- 授予用户创建数据库的权限
```sql
GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost';
FLUSH PRIVILEGES;
```

## 扩展测试

### 添加新的测试场景
1. 在`MockDataGenerator`中添加新的SQL脚本
2. 创建新的数据不一致模式
3. 更新测试规则配置
4. 运行集成测试验证

### 性能测试
```java
// 可扩展支持大数据量测试
generator.generateLargeDataset(100000); // 生成10万条测试数据
```

### 多数据库类型测试
```java
// 支持测试其他数据库类型
MockDataGenerator pgGenerator = new MockDataGenerator("postgresql-source");
pgGenerator.generateTestDatabases(...);
```

## 测试结果示例

成功运行测试后，控制台输出类似：
```
[INFO] 开始数据一致性集成测试
[INFO] 步骤1: 生成测试数据...
[INFO] 步骤2: 创建一致性规则...
[INFO] 步骤3: 执行一致性检查...
[INFO] 步骤4: 验证结果...
[INFO] ==========================================
[INFO] 一致性检查结果:
[INFO] 规则ID: integration-test-rule
[INFO] 状态: PARTIAL_SUCCESS
[INFO] 总记录数: 24
[INFO] 一致记录数: 12
[INFO] 不一致记录数: 12
[INFO] 已解决记录数: 12
[INFO] 字段差异统计:
[INFO]   字段 'age': 3 处差异
[INFO]   字段 'salary': 4 处差异
[INFO]   字段 'department': 2 处差异
[INFO] 报告路径: ./test-results/consistency_report_integration-test-rule_20250101_120000.html
[INFO] ==========================================
[INFO] 测试通过: 成功检测到数据不一致
[INFO] 集成测试完成！
```