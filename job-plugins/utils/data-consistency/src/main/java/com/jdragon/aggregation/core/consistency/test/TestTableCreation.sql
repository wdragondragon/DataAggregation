-- 数据一致性对比系统测试用建表语句
-- 用于模拟多个数据源的数据不一致场景

-- ============================================
-- 1. 创建测试数据库（模拟不同数据源）
-- ============================================

-- 主数据库（高可信度源）
CREATE DATABASE IF NOT EXISTS primary_db;
USE primary_db;

-- 备份数据库（中等可信度源）
CREATE DATABASE IF NOT EXISTS backup_db;
USE backup_db;

-- 数据仓库（低可信度源）
CREATE DATABASE IF NOT EXISTS warehouse_db;
USE warehouse_db;

-- ============================================
-- 2. 用户表创建（三个数据库中的表结构相同）
-- ============================================

-- 主数据库用户表
USE primary_db;
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    age INT,
    salary DECIMAL(10, 2),
    department VARCHAR(50),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 备份数据库用户表
USE backup_db;
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    age INT,
    salary DECIMAL(10, 2),
    department VARCHAR(50),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 数据仓库用户表（注意：字段名不同，用于测试字段映射）
USE warehouse_db;
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    age INT,
    salary DECIMAL(10, 2),
    dept VARCHAR(50),  -- 注意：这里字段名是dept而不是department
    email VARCHAR(100),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- 3. 订单表创建（三个数据库中的表结构相同）
-- ============================================

-- 主数据库订单表
USE primary_db;
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    order_number VARCHAR(50) NOT NULL UNIQUE,
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    order_date DATE,
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    INDEX idx_user_id (user_id),
    INDEX idx_order_date (order_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 备份数据库订单表
USE backup_db;
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    order_number VARCHAR(50) NOT NULL UNIQUE,
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    order_date DATE,
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    INDEX idx_user_id (user_id),
    INDEX idx_order_date (order_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 数据仓库订单表
USE warehouse_db;
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    order_number VARCHAR(50) NOT NULL UNIQUE,
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    order_date DATE,
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_order_date (order_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- 4. 插入测试数据（模拟数据不一致场景）
-- ============================================

-- 4.1 主数据库用户数据（最准确的数据）
USE primary_db;
INSERT INTO users (user_id, username, age, salary, department, email) VALUES
(1, 'zhangsan', 30, 15000.00, 'Technology', 'zhangsan@company.com'),
(2, 'lisi', 28, 12000.00, 'Marketing', 'lisi@company.com'),
(3, 'wangwu', 35, 18000.00, 'Sales', 'wangwu@company.com'),
(4, 'zhaoliu', 32, 16000.00, 'HR', 'zhaoliu@company.com'),
(5, 'sunqi', 29, 13000.00, 'Finance', 'sunqi@company.com'),
(6, 'zhouba', 31, 14000.00, 'Technology', 'zhouba@company.com'),
(7, 'wujiu', 27, 11000.00, 'Marketing', 'wujiu@company.com'),
(8, 'zhengshi', 33, 17000.00, 'Sales', 'zhengshi@company.com');

-- 主数据库订单数据
INSERT INTO orders (user_id, order_number, amount, status, order_date, shipping_address) VALUES
(1, 'ORD-2023001', 5000.00, 'COMPLETED', '2023-01-15', 'Beijing, Haidian District'),
(1, 'ORD-2023002', 3000.00, 'SHIPPED', '2023-02-20', 'Beijing, Haidian District'),
(2, 'ORD-2023003', 8000.00, 'PROCESSING', '2023-03-10', 'Shanghai, Pudong District'),
(3, 'ORD-2023004', 12000.00, 'COMPLETED', '2023-01-25', 'Guangzhou, Tianhe District'),
(4, 'ORD-2023005', 4500.00, 'CANCELLED', '2023-02-28', 'Shenzhen, Nanshan District'),
(5, 'ORD-2023006', 6000.00, 'SHIPPED', '2023-03-15', 'Hangzhou, Xihu District');

-- 4.2 备份数据库用户数据（部分数据与主数据库不一致）
USE backup_db;
INSERT INTO users (user_id, username, age, salary, department, email) VALUES
(1, 'zhangsan', 31, 15000.00, 'Technology', 'zhangsan@company.com'),        -- 年龄不一致：30 vs 31
(2, 'lisi', 28, 12500.00, 'Marketing', 'lisi@company.com'),                -- 工资不一致：12000 vs 12500
(3, 'wangwu', 35, 18000.00, 'Sales', 'wangwu@company.com'),                -- 一致
(4, 'zhaoliu', 32, 16000.00, 'HR', 'zhaoliu@company.com'),                 -- 一致
(5, 'sunqi', 29, 13000.00, 'Finance', 'sunqi@company.com'),                -- 一致
(6, 'zhouba', 31, 14200.00, 'Technology', 'zhouba@company.com'),           -- 工资不一致：14000 vs 14200
(7, 'wujiu', 27, 11000.00, 'Marketing', 'wujiu@company.com'),              -- 一致
(8, 'zhengshi', 33, 17000.00, 'Sales', 'zhengshi@company.com');            -- 一致

-- 备份数据库订单数据（部分数据不一致）
INSERT INTO orders (user_id, order_number, amount, status, order_date, shipping_address) VALUES
(1, 'ORD-2023001', 5000.00, 'COMPLETED', '2023-01-15', 'Beijing, Haidian District'),
(1, 'ORD-2023002', 3100.00, 'SHIPPED', '2023-02-20', 'Beijing, Haidian District'),   -- 金额不一致：3000 vs 3100
(2, 'ORD-2023003', 8000.00, 'PROCESSING', '2023-03-10', 'Shanghai, Pudong District'),
(3, 'ORD-2023004', 12000.00, 'COMPLETED', '2023-01-25', 'Guangzhou, Tianhe District'),
(4, 'ORD-2023005', 4500.00, 'CANCELLED', '2023-02-28', 'Shenzhen, Nanshan District'),
(5, 'ORD-2023006', 6000.00, 'DELIVERED', '2023-03-15', 'Hangzhou, Xihu District');   -- 状态不一致：SHIPPED vs DELIVERED

-- 4.3 数据仓库用户数据（更多不一致，且字段名不同）
USE warehouse_db;
INSERT INTO users (user_id, username, age, salary, dept, email) VALUES
(1, 'zhangsan', 30, 15000.00, 'Tech', 'zhangsan@company.com'),             -- 部门不一致：Technology vs Tech
(2, 'lisi', 28, 12000.00, 'Marketing', 'lisi@company.com'),                -- 一致
(3, 'wangwu', 36, 18200.00, 'Sales', 'wangwu@company.com'),                -- 年龄和工资都不一致：35/18000 vs 36/18200
(4, 'zhaoliu', 32, 16000.00, 'Human Resource', 'zhaoliu@company.com'),     -- 部门不一致：HR vs Human Resource
(5, 'sunqi', 29, 13000.00, 'Finance', 'sunqi@company.com'),                -- 一致
(6, 'zhouba', 31, 14000.00, 'Technology', 'zhouba@company.com'),           -- 一致
(7, 'wujiu', 27, 11500.00, 'Marketing', 'wujiu@company.com'),              -- 工资不一致：11000 vs 11500
(8, 'zhengshi', NULL, 17000.00, 'Sales', 'zhengshi@company.com');          -- 年龄为NULL

-- 数据仓库订单数据
INSERT INTO orders (user_id, order_number, amount, status, order_date, shipping_address) VALUES
(1, 'ORD-2023001', 5000.00, 'COMPLETED', '2023-01-15', 'Beijing, Haidian District'),
(1, 'ORD-2023002', 3000.00, 'SHIPPED', '2023-02-20', 'Beijing, Haidian District'),
(2, 'ORD-2023003', 8100.00, 'PROCESSING', '2023-03-10', 'Shanghai, Pudong District'),  -- 金额不一致：8000 vs 8100
(3, 'ORD-2023004', 12000.00, 'COMPLETED', '2023-01-25', 'Guangzhou, Tianhe District'),
(4, 'ORD-2023005', 4550.00, 'CANCELLED', '2023-02-28', 'Shenzhen, Nanshan District'),  -- 金额不一致：4500 vs 4550
(5, 'ORD-2023006', 6000.00, 'SHIPPED', '2023-03-15', 'Hangzhou, Xihu District');

-- ============================================
-- 5. 查询验证数据
-- ============================================

-- 验证主数据库数据
USE primary_db;
SELECT 'Primary DB - Users' as TableName;
SELECT * FROM users ORDER BY user_id;
SELECT 'Primary DB - Orders' as TableName;
SELECT * FROM orders ORDER BY order_id;

-- 验证备份数据库数据
USE backup_db;
SELECT 'Backup DB - Users' as TableName;
SELECT * FROM users ORDER BY user_id;
SELECT 'Backup DB - Orders' as TableName;
SELECT * FROM orders ORDER BY order_id;

-- 验证数据仓库数据
USE warehouse_db;
SELECT 'Warehouse DB - Users' as TableName;
SELECT * FROM users ORDER BY user_id;
SELECT 'Warehouse DB - Orders' as TableName;
SELECT * FROM orders ORDER BY order_id;

-- ============================================
-- 6. 数据一致性检查示例查询
-- ============================================

-- 6.1 检查用户数据在不同数据库中的差异
SELECT 
    p.user_id,
    p.username,
    p.age as age_primary,
    b.age as age_backup,
    w.age as age_warehouse,
    p.salary as salary_primary,
    b.salary as salary_backup,
    w.salary as salary_warehouse,
    p.department as dept_primary,
    b.department as dept_backup,
    w.dept as dept_warehouse
FROM primary_db.users p
LEFT JOIN backup_db.users b ON p.user_id = b.user_id
LEFT JOIN warehouse_db.users w ON p.user_id = w.user_id
ORDER BY p.user_id;

-- 6.2 检查订单数据差异
SELECT 
    p.order_id,
    p.order_number,
    p.amount as amount_primary,
    b.amount as amount_backup,
    w.amount as amount_warehouse,
    p.status as status_primary,
    b.status as status_backup,
    w.status as status_warehouse
FROM primary_db.orders p
LEFT JOIN backup_db.orders b ON p.order_number = b.order_number
LEFT JOIN warehouse_db.orders w ON p.order_number = w.order_number
ORDER BY p.order_id;

-- ============================================
-- 7. PostgreSQL兼容版本（如果需要）
-- ============================================
/*
-- PostgreSQL版本建表语句（主要差异）：
-- 1. 使用SERIAL代替AUTO_INCREMENT
-- 2. 引号处理不同
-- 3. 引擎声明不同

CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    age INT,
    salary DECIMAL(10, 2),
    department VARCHAR(50),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_username ON users(username);
*/

-- ============================================
-- 8. 清理脚本（测试完成后清理）
-- ============================================
/*
-- 删除测试数据（谨慎使用）
DROP DATABASE IF EXISTS primary_db;
DROP DATABASE IF EXISTS backup_db;
DROP DATABASE IF EXISTS warehouse_db;
*/