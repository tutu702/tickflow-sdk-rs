# TickFlow Rust SDK

[![Rust Edition](https://img.shields.io/badge/rust-edition%202024-orange.svg)](https://doc.rust-lang.org/edition-guide/) [![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE) [![Docs](https://img.shields.io/badge/docs-docs.tickflow.org-blue)](https://docs.tickflow.org)

TickFlow Rust SDK 是参考 TickFlow 官方 Python SDK 实现的 Rust 异步客户端，覆盖行情、K 线、盘口、标的、标的池、交易所与财务数据等接口，支持 A 股、ETF、港股、美股等市场。

> **完整文档**：<https://docs.tickflow.org>

---

## 安装

将 TickFlow 添加到 `Cargo.toml`：

```bash
cargo add tickflow
```

或手动添加：

```toml
[dependencies]
tickflow = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

SDK 需要 Rust 1.96+（edition 2024），并基于 Tokio 异步运行时构建。

---

## 快速开始

### 免费服务（无需注册）

如果你只需要日 K 线数据、标的信息、交易所与标的池查询（不需要实时行情、分钟 K 线与盘口），可以直接使用免费服务：

```rust
use tickflow::client::TickFlow;
use tickflow::resources::klines::KlinesBuilderExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 使用免费服务（无需 API key）
    let tf = TickFlow::free()?;

    // 查询日 K 线数据
    let klines = tf.klines().get("600000.SH").count(100).send().await?;
    if let Some(last) = klines.last_close() {
        println!("最新收盘价: {last}");
    }

    // 查询标的信息
    let instruments = tf
        .instruments()
        .batch(["600000.SH", "000001.SZ"])
        .send()
        .await?;
    println!("{instruments:#?}");

    // 查询所有交易所
    let exchanges = tf.exchanges().list().await?;
    for ex in exchanges {
        println!("{}: {} ({} 个标的)", ex.exchange, ex.region.as_str(), ex.count);
    }

    Ok(())
}
```

**免费服务特点：**

- ✅ 无需注册，直接使用
- ✅ 提供历史日 K 线数据（1d、1w、1M、1Q、1Y）
- ✅ 提供标的信息、交易所、标的池查询
- ❌ 不提供实时行情
- ❌ 不提供分钟级 K 线（1m、5m、15m、30m、60m）
- ❌ 不提供盘口与日内分时数据
- ❌ 不提供财务数据
- ⚠️ 日 K 数据为历史数据，盘中不会实时更新

如果需要实时行情、盘中实时更新的 K 线、盘口或财务数据，请使用完整服务。

### 完整服务（需注册）

#### 1. 获取 API Key

访问 [tickflow.org](https://tickflow.org/auth/register?ref=LHJNYB5ZVC) 注册后，在控制台一键生成你的 API Key。

#### 2. 配置认证

**方式一：直接传入**

```rust
use tickflow::client::TickFlow;

let tf = TickFlow::new("your-api-key")?;
```

**方式二：通过 Builder**

```rust
use tickflow::client::TickFlow;
use std::time::Duration;

let tf = TickFlow::builder()
    .api_key("your-api-key")
    .timeout(Duration::from_secs(60))
    .build()?;
```

**方式三：环境变量**

```bash
export TICKFLOW_API_KEY="your-api-key"
# 可选：自定义 API endpoint
# export TICKFLOW_BASE_URL="https://api.tickflow.org"
```

```rust
use tickflow::client::TickFlow;

// 自动读取 TICKFLOW_API_KEY 环境变量
let tf = TickFlow::builder().build()?;
```

#### 3. 发送请求

```rust
use std::env;
use tickflow::client::TickFlow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("TICKFLOW_API_KEY").expect("TICKFLOW_API_KEY must be set");
    // 使用完整服务（需要 API key）
    let tf = TickFlow::new(api_key)?;

    // 获取沪深 A 股实时行情
    let quotes = tf.quotes().batch_symbols(["600000.SH", "000001.SZ"]).send().await?;
    for (symbol, q) in &quotes {
        println!("{symbol}: last={}", q.last_price);
    }

    Ok(())
}
```

如果看到股票价格输出，说明 SDK 已配置成功。

**完整服务优势：**

- ✅ 实时行情数据
- ✅ 分钟级 K 线（1m、5m、10m、15m、30m、60m）
- ✅ 日内分时数据
- ✅ 盘口（买卖十档）
- ✅ 财务数据（利润表、资产负债表、现金流量表、关键指标、股本）
- ✅ 更高的调用频率

---

## 客户端配置

`TickFlowBuilder` 暴露以下配置项：

| 方法 | 默认值 | 说明 |
|------|--------|------|
| `api_key(key)` | `TICKFLOW_API_KEY` 环境变量 | 认证用 API Key |
| `base_url(url)` | `https://api.tickflow.org` | 自定义 API endpoint |
| `free_tier()` | — | 切换到免费服务（`https://free-api.tickflow.org`，无需 API Key） |
| `timeout(d)` | `30s` | 单请求超时 |
| `build()` | — | 构造 `TickFlow` 客户端 |

环境变量：

- `TICKFLOW_API_KEY` — 启动时自动注入 API Key。
- `TICKFLOW_BASE_URL` — 启动时自动注入自定义 endpoint。

```rust
use tickflow::client::TickFlow;
use std::time::Duration;

let tf = TickFlow::builder()
    .api_key("your-api-key")
    .base_url("https://api.tickflow.org")
    .timeout(Duration::from_secs(60))
    .build()?;
```

---

## License

MIT