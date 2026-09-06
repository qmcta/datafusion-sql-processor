---
name: audit_and_test
description: Run automated tests, analyze compiler/lint failures, generate test_results.log, and control autonomous verification loop decisions.
---

# Skill: Audit and Test Code

## Objective
実装されたコードに対してテストやリンターを実行し、品質基準および `spec.md` を満たしているか検証します。エラー発生時は自律修正のためのフィードバックログを生成し、ループの継続／終了を判定します。

## Role
QA Agent (QA & Verification Engineer)

## Inputs
- `spec.md`: 仕様書および品質基準
- `src/`, `tests/`: 対象ソースコードおよびテストコード
- `iteration_count`: 現在のループ回数（1〜5）

## Instructions

### 1. 検証コマンドの実行
以下の検証コマンドを順番に実行します（ホスト環境または Docker コンテナ内 `docker exec -i rustdev cargo ...`）：

```bash
# 1. コンパイル・型チェック
cargo check

# 2. 自動テストの実行
cargo test

# 3. リンター・静的解析
cargo clippy --all-targets -- -D warnings
```

### 2. 判定ロジック (Evaluation & Loop Decision)

#### ケース A: 全検証が PASS した場合（成功終了）
- 既存の `test_results.log` があれば削除します。
- 以下の完了通知を出力し、**ループを正常終了（STOP）** します。
  > `🎉 Verification Loop PASSED: All compiler checks, unit tests, and clippy lints passed successfully.`

#### ケース B: エラーまたはテスト失敗が発生し、かつ試行回数が上限未満の場合（ループ継続）
- 失敗したコマンドの標準出力・標準エラー出力を取得します。
- プロジェクトルートに `test_results.log` を作成・上書き保存します：
  - 発生日時および試行回数（`Iteration: X / 5`）
  - 失敗したコマンド名
  - エラーメッセージ、対象ファイル名、行番号、panic トレース
  - QA Agent による原因分析と Developer Agent への具体的修正指示
- ループカウンタを +1 インクリメントし、**Developer Agent（`implement` スキル）へ制御を戻して修正を要求**します。

#### ケース C: リトライ上限（最大 5 回）に達した場合（エスカレーション停止）
- 5 回連続で検証をパスできなかった場合、自律ループを停止します。
- `test_results.log` の内容を要約し、人間にレビューを求める通知を出力して**停止（HALT）**します：
  > `⚠️ Verification Loop HALTED: Maximum retry limit (5) reached. Human intervention required.`
  > 未解決のエラー概要と試行履歴を提示し、ユーザーの指示を待ちます。

