# **🤖 Project Agents & Skills (v2)**

This repository integrates AI-driven agents and specialized skills to automate technical workflows.

## **📂 Structure**

All agent-related configurations, design guidelines, and skill definitions are stored in the .agents directory:

.agents/
├── AGENTS.md         <-- Main entry point
├── DESIGN.md         <-- Visual guidelines and CSS templates
└── skills/
    └── generate_slides/
        └── SKILL.md  <-- Logic and transformation rules

## **🛠️ Available Skills**

### **1. Generate Slides (generate_slides)**
- **Goal**: Automatically transform technical documents (Markdown, Source Code, or Text files) into a professional, Google Cloud-styled Marp presentation.
- **Design Integration**: This skill now references .agents/DESIGN.md for all styling and branding rules.
- **Key Features**:
  - Implements official design language via DESIGN.md.
  - Handles long documents with intelligent splitting.
  - Maintains 100% technical integrity of code and scenarios.

## **🚀 How to Use**
1. **Reference the Skill**: Ask the assistant to "Generate Slides using the current project's skills".
2. **Automatic Loading**: The agent will load SKILL.md for the procedure and DESIGN.md for the visual style.