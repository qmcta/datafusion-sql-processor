# SKILL: Technical Document to Google Cloud Style Marp Presentation

## 🎯 Goal
Automatically transform technical Markdown documentation (like `README.md`) into a professional, high-impact slide deck using Marp, adhering strictly to Google Cloud's official design language and aesthetics.

## 🛠️ Pre-requisites
- **Source**: A structured Markdown file.
- **Renderer**: [Marp](https://marp.app/) (CLI or VS Code Extension).
- **Design Assets**: Access to Google Cloud brand colors and professional tech-themed background images.

## 🤖 Agent Instructions (Prompt Template)

### 🧩 Input Parameters
- **Source File**: The target Markdown file to convert. 
- **Output File**: The filename for the generated Marp presentation.
- **Requirement**: **ALWAYS ask the user** which file they would like to convert **AND** what the output filename should be. If they don't specify an output name, use `<source_filename>_presentation.md` as the default.

### 📋 Execution Logic
When tasked with creating a presentation, use the following system prompt logic:

### 1. Style Definition (CSS)
Embed the following CSS block in the Marp frontmatter to ensure Google Cloud branding:

```css
marp: true
theme: default
paginate: true
style: |
  section {
    font-family: 'Inter', 'Roboto', 'Segoe UI', sans-serif;
    color: #3c4043;
    background-color: #ffffff;
    font-size: 24px;
    padding: 40px;
  }
  h1 { color: #4285f4; border-bottom: 2px solid #4285f4; padding-bottom: 10px; }
  h2 { color: #1a73e8; }
  code { background-color: #f1f3f4; color: #d93025; border-radius: 4px; padding: 2px 6px; }
  pre { background-color: #202124; color: #f8f9fa; border-radius: 8px; padding: 20px; font-size: 18px; }
  .title-slide {
    background-image: linear-gradient(rgba(255,255,255,0.8), rgba(255,255,255,0.8)), url('tech_background.png');
    background-size: cover;
    text-align: center;
    display: flex;
    flex-direction: column;
    justify-content: center;
  }
  .section-title { background-color: #4285f4; color: white; display: flex; align-items: center; justify-content: center; }
  .section-title h1 { color: white; border-bottom: none; font-size: 50px; }
  .badge { display: inline-block; padding: 4px 12px; border-radius: 16px; font-size: 14px; font-weight: bold; }
  .badge-yellow { background-color: #fef7e0; color: #b26a00; }
```

### 2. Transformation Workflow
1. **File Selection**: Check the provided parameters for a source filename and an output filename. 
   - **If Source is missing**: 
     - Use your tools to list all Markdown (`.md`), Text (`.txt`), and PDF (`.pdf`) files in the current workspace.
     - Present these as candidates to the user and **ask them to choose or specify a different file**.
     - **STOP** until the user confirms the source file.
   - **If Output is missing**: Default to `[SOURCE]_presentation.md`.
2. **Content Extraction**: Identify H1, H2, and H3 headers. Additionally, identify **bolded section markers** (e.g., `**Scenario X:**` or `**シナリオX:**`) as sub-slide titles. Map H1 to the Title Slide, H2s to Section Title Slides, and H3s/Bold markers/Body to Content Slides.
3. **Visual Hierarchy**:
   - **Title Slide**: Project Name (H1) + Subtitle (H2).
   - **Agenda**: Auto-generate based on H2 headers.
   - **Step-by-Step**: Use code blocks for commands. Use columns for side-by-side comparisons.
   - **Scenario Preservation**: Every bolded scenario (e.g., "**Scenario G**") MUST have its own slide or a dedicated part of a slide. NEVER omit these.
3. **Emphasis**: Use the `.badge` class for warnings, tips, and critical information (e.g., WSL2 filesystem restrictions).
4. **Content Integrity**: Do NOT omit technical details, warnings, or explanatory text from the source. 
   - **No Omissions**: Ensure every section, sub-section, and **example scenario** is represented.
   - **Slide Splitting**: If a single section is too long for one slide (more than 6-8 lines), split it across multiple slides (e.g., "Part 1", "Part 2") to maintain readability without losing information.
   - **Code & Table Preservation**: Maintain all code blocks, tables, and comparison charts. If a file is large, prioritize technical examples over redundant prose, but never drop the core logic.

## 📝 Example Reproducible Prompt

> "Transform the [SOURCE_FILE] into [OUTPUT_FILE] (default: `[SOURCE_FILE]_presentation.md`) for Marp.
> Use a Google Cloud theme: White backgrounds for content, Blue (#4285F4) for section headers, and 'Google Sans' (Roboto) font.
> Include a Title slide with a generated tech background.
> Ensure every H2 from the source file gets a 'Section Title' slide with a solid blue background.
> Convert technical instructions into clear code blocks and summarized bullets."

## 🚀 Key Success Factors
- **Whitespace**: Maintain clean layouts with plenty of padding.
- **Consistency**: Use `#4285f4` as the primary accent color.
- **Clarity**: High contrast for code blocks (Dark background, Light text).
- **Interactivity**: Include a "Thank You & Q&A" slide at the end.

## 🪝 Hooks Definition

### Pre-execution
1. 読み込むファイルが非常に長い（20,000文字超）場合でも、技術的な「シナリオ」や「コード例」は絶対に省略せず、説明文を簡潔にする（要約する）ことでスライドに収めること。
2. ファイル内に機密情報（パスワード等）と思われる文字列がある場合、警告を出してマスクすること。

### Post-execution
1. 生成したMarp用Markdownの構文エラーをチェックすること。
2. スライドが10枚を超える場合は、末尾に「Appendix（付録）」セクションを自動生成すること。