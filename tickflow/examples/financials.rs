use std::collections::HashMap;
use std::env;

use tickflow::client::TickFlow;
use tickflow::model::{BalanceSheetRecord, IncomeRecord};
use tickflow::resources::financials::{FinancialResponse, StatementParams};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("TICKFLOW_API_KEY").expect("TICKFLOW_API_KEY must be set");
    let tf = TickFlow::new(api_key)?;

    let symbols = ["000001.SZ"];

    // ─────────────────────────────────────────────────────────────────────────
    // 1. 利润表（Income）—— Raw 形式
    // ─────────────────────────────────────────────────────────────────────────
    let raw = tf.financials().income(symbols, None).await?;
    let FinancialResponse::Raw(map) = raw else {
        unreachable!("as_dataframe 未开启");
    };
    print_income_raw(&map);

    // ─────────────────────────────────────────────────────────────────────────
    // 2. 利润表 —— DataFrame 形式
    // ─────────────────────────────────────────────────────────────────────────
    let resp = tf
        .financials()
        .income(
            symbols,
            Some(
                StatementParams::new()
                    .start_date("2023-01-01")
                    .end_date("2024-12-31")
                    .as_dataframe(true),
            ),
        )
        .await?;
    if let FinancialResponse::DataFrame(df) = resp {
        println!("\n== 000001.SZ 利润表 (2023-2024, {} 行) ==", df.height());
        println!("{df}");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. 资产负债表（Balance Sheet）
    // ─────────────────────────────────────────────────────────────────────────
    let resp = tf
        .financials()
        .balance_sheet(symbols, Some(StatementParams::new().latest(true)))
        .await?;
    if let FinancialResponse::Raw(map) = resp {
        print_balance_sheet(&map);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. 现金流量表（Cash Flow）
    // ─────────────────────────────────────────────────────────────────────────
    let resp = tf
        .financials()
        .cash_flow(
            ["600000.SH", "000001.SZ"],
            Some(StatementParams::new().as_dataframe(true)),
        )
        .await?;
    if let FinancialResponse::DataFrame(df) = resp {
        println!("\n== 两标的现金流量表 ({} 行) ==", df.height());
        println!("{df}");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. 财务指标（Metrics）—— 一次拿多只标的
    // ─────────────────────────────────────────────────────────────────────────
    let resp = tf
        .financials()
        .metrics(
            ["600000.SH", "000001.SZ"],
            Some(StatementParams::new().latest(true).as_dataframe(true)),
        )
        .await?;
    if let FinancialResponse::DataFrame(df) = resp {
        println!("\n== 两标的最新财务指标 ({} 行) ==", df.height());
        println!("{df}");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 6. 股本变动（Shares）
    // ─────────────────────────────────────────────────────────────────────────
    let resp = tf
        .financials()
        .shares(
            ["600000.SH"],
            Some(
                StatementParams::new()
                    .start_date("2022-01-01")
                    .as_dataframe(true),
            ),
        )
        .await?;
    if let FinancialResponse::DataFrame(df) = resp {
        println!("\n== 600000.SH 股本变动 (2022 起, {} 行) ==", df.height());
        println!("{df}");
    }

    Ok(())
}

fn print_income_raw(map: &HashMap<String, Vec<IncomeRecord>>) {
    println!("\n== 利润表 (Raw) ==");
    for (symbol, records) in map {
        println!("{symbol}: 共 {} 期，前 2 期 ↓", records.len());
        for r in records.iter().take(2) {
            println!("  {r:?}");
        }
        if records.len() > 2 {
            println!("  … 其余 {} 期省略", records.len() - 2);
        }
    }
}

fn print_balance_sheet(map: &HashMap<String, Vec<BalanceSheetRecord>>) {
    println!("\n== 最新资产负债表 (Raw) ==");
    for (symbol, records) in map {
        if let Some(latest) = records.last() {
            println!("{symbol}:");
            println!("  {latest:?}");
        }
    }
}
