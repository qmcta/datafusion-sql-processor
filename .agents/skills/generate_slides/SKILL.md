# **SKILL: Technical Document to Google Cloud Style Marp Presentation (v2)**

## **🎯 Goal**
Automatically transform technical documents (Markdown, Source Code, or Text files) into a professional slide deck using Marp, delegating visual styling to the project's central design system.

## **🛠️ Pre-requisites**
- **Source**: A technical document (Markdown, Source Code, or Plain Text).
- **Style Guide**: Must have access to .agents/DESIGN.md.

## **🤖 Agent Instructions**

### **🧩 Input Parameters**
- **Source File**: Target technical document (Markdown, source code, or text file).
- **Output File**: Desired filename (default: [SOURCE]_presentation.md).

### **📋 Execution Logic**

#### **1. Design & Style Retrieval (CRITICAL)**
Before generating any content, the agent MUST:
1. Open and read .agents/DESIGN.md.
2. Extract the CSS block provided in the "CSS Template (Marp)" section.
3. Use the color codes and class names (e.g., .title-slide, .section-title) defined therein.

#### **2. Transformation Workflow**
- **Frontmatter**: Start the output file with the CSS block retrieved from DESIGN.md.
- **Title Slide**: Map the H1 header and apply the `.title-slide` class.
- **Section Divider**: For each H2 header, create a slide with the `.section-title` class.
- **Content Slides**: Map H3s and body text. Use `.badge` classes from DESIGN.md for emphasis.
- **Handling Source Code/Text**: 
  - If the input is source code or text without markdown headers, the agent MUST:
    - Determine the structure (e.g., classes, functions, or logical blocks).
    - Create a Title Slide with the file name and a brief summary.
    - Create Section Divider slides for major components.
    - Present code snippets in formatted code blocks with explanatory text.

#### **3. Content Integrity**
- Maintain all code blocks, tables, and scenario sections without omission.
- Split long sections into multiple slides to maintain readability.

## **🪝 Hooks Definition**

### **Pre-execution**
1. 読み込むファイルが非常に長い（20,000文字超）場合でも、技術的な「シナリオ」や「コード例」は絶対に省略せず、説明文を簡潔にする（要約する）ことでスライドに収めること。
2. ファイル内に機密情報（パスワード等）と思われる文字列がある場合、警告を出してマスクすること。

### **Post-execution**
1. 生成したMarp用Markdownの構文エラーをチェックすること。
2. スライドが10枚を超える場合は、末尾に「Appendix（付録）」セクションを自動生成すること。