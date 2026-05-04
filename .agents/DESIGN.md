# **DESIGN: Google Cloud Style Presentation Guidelines**

## **🎨 Visual Identity**

Define the visual language for all generated presentations to ensure brand consistency.

### **Colors**
- **Primary (Google Blue)**: #4285f4 - Used for main headers, section backgrounds, and primary accents.
- **Secondary (Dark Blue)**: #1a73e8 - Used for sub-headers and links.
- **Background**: #ffffff - Clean white for content slides.
- **Text**: #3c4043 - Dark grey for optimal readability.
- **Code Background**: #202124 - Dark background for syntax highlighting.
- **Code Text**: #f8f9fa - Light text for code blocks.
- **Alert/Badge**: #fef7e0 background with #b26a00 text for warnings/tips.

### **Typography**
- **Primary Font**: 'Google Sans', 'Inter', 'Roboto', sans-serif.
- **Body Size**: 24px for standard text.
- **Code Size**: 18px for monospaced blocks.

### **CSS Template (Marp)**
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