# Velocity AI Project

This is the central repository to collate the research and development work of the **Velocity AI Project**. This repo serves as the single source of truth for backend logic, frontend interfaces, and experimental documentation.

---

## 📂 Repository Contents

| Directory | Description |
| :--- | :--- |
| **`.github/workflows`** | CI/CD pipelines for automated testing and deployment. |
| **`Documentation/`** | Research papers, tutorials, technical specs, and project architecture notes. |
| **`Prompts/`** | A collection of engineered prompts used for LLM research and R&D. |
| **`Python/`** | Backend source code, data processing scripts, and AI logic. |
| **`React/`** | The frontend source codes. |
| **`.idea/`** | IDE-specific configuration files (JetBrains ecosystem). |
| **`.gitignore`** | Instructions for Git on which files to ignore. |

---

## 🚀 Getting Started

To get a local copy up and running, follow these steps:

1. **Clone the Repository**
   \`\`\`bash
   git clone https://github.com/LatentviewIT/Velocity-AI
   \`\`\`
2. **Environment Setup**
    * **Backend:** Navigate to \`/Python\` and install necessary dependencies.
    * **Frontend:** Navigate to \`/React/vel-csv-pipeline-ui\` and run \`npm install\`.

---

## 🤝 Contribution Guidelines

To maintain a clean and functional codebase, all contributors must follow these steps:

* **Documentation:** Ensure any new research or reusable assets are reflected in the \`Documentation/\` folder.
* **Prompt Management:** Store all AI system prompts in the \`Prompts/\` directory to maintain version control over LLM behavior.
* **Python:** Store all the Python scripts and snippets in the \`Python/\` directory.
* **React:** Store all the frontend scripts and snippets in the \`React/\` directory.
---

## 🛠 Version Control Practices

To minimize technical debt and merge conflicts, please adhere to the following instructions. 

### 1. Sync Frequently
**Always** perform a \`git pull\` before you begin working and immediately before you \`push\`. This ensures you are working on the most recent version of the code and avoids mid-air collisions.

### 2. Atomic Commits
Try to work on **individual files** whenever possible. Avoid working on the same files simultaneously to prevent complex merge conflicts.

### 3. Feature Branching & PRs
When working on the same file or a major feature:
* **Create a Branch:** \`git checkout -b feature/your-feature-name\`
* **Modify:** Work on your modifications within this branch.
* **Pull Request:** Raise a Pull Request (PR) for review.
* **Review:** All PRs must be reviewed and approved by the **Project Lead** before merging into the main branch.
