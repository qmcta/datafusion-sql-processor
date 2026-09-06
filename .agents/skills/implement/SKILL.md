---
name: implement
description: Implement or modify Rust source code based on spec.md requirements and feedback from test_results.log.
---

# Skill: Implement Requirements

## Objective
`spec.md` に記載された仕様に基づき、必要なソースコードを作成・更新します。直前にテストやビルドの失敗ログ（`test_results.log`）が存在する場合は、その原因を特定してコードを自律的に修正・リファクタリングします。

## Role
Developer Agent (Architect & Developer)

## Inputs
- `spec.md`: 仕様書（機能要件、CLI仕様、エラーハンドリング基準）
- `src/`: Rust ソースコード (`main.rs` など)
- `Cargo.toml`: 依存クレート設定
- `test_results.log`: 直前のテスト・ビルド失敗ログ（存在する場合）

## Instructions
1. **要件と現状の確認**:
   - `spec.md` を確認し、実装または変更対象の要件を把握します。
   - 関連する既存コード（`src/main.rs`, `Cargo.toml`, 関連モジュール等）を確認します。
2. **エラーログの分析 (修正ループ時)**:
   - 作業領域に `test_results.log` が存在するか確認します。
   - ログが存在する場合、Rust コンパイラのエラー（borrow checker, lifetime, type mismatch 等）やテストのアサーション失敗（line number, panic message 等）を詳細に解析します。
   - 単なる一時しのぎではなく、`spec.md` の品質基準を満たす抜本的な修正アプローチを策定します。
3. **コードの実装・修正**:
   - 対象ファイルに対して適切な修正を適用します。
   - コマンドライン引数 (`Args`), データ処理ロジック, フォーマット出力 (`csv`, `parquet`, `jsonl`), エラーハンドリングが `spec.md` と整合していることを担保します。
4. **引き渡し**:
   - 実装・修正完了後、変更点の要約を出力し、QA Agent（`audit_and_test` スキル）へ検証を依頼します。

