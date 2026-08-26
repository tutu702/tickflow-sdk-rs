## [0.1.0] - 2026-08-26

### 🚀 Features

- Initialize tickflow Rust SDK with workspace, client, and quote resource
- *(klines)* [**breaking**] Add kline resource with builder API and query support
- *(klines)* [**breaking**] Add batch kline queries with shared builder trait
- *(quotes)* [**breaking**] Introduce builder API with single/batch and universe support
- *(instruments)* [**breaking**] Add instruments resource with builder API and shared batch chunk constant
- *(universes)* [**breaking**] Add universes resource with builder API
- *(klines)* [**breaking**] Add intraday and ex-factors endpoints with builder API
- *(depth)* Add market depth resource and models
- *(exchanges)* Add exchanges resource with list and instruments endpoints
- *(financials)* [**breaking**] Add financials resource with income, balance-sheet, cash-flow, metrics, and shares endpoints

### 🐛 Bug Fixes

- *(universes)* Correct batch endpoint to /v1/universes/batch and add example

### 💼 Other

- Initial commit
- *(workspace)* Centralize dependencies and package metadata in workspace root

### 📚 Documentation

- *(examples)* Add free-tier usage and quotes resource examples
- *(readme)* Add project README with installation, quick start, and client config
- *(examples)* Add klines, intraday, and instruments usage examples
