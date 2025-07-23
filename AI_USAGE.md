# AI Usage Documentation

This document details the AI tools used in the development of this project, the prompts that were most effective, any mistakes made by the AI and how they were fixed, and an estimate of the time saved through AI assistance.

## AI Tools Used

- **Gemini (Google)**: Used as an interactive CLI agent for code generation, refactoring, debugging, and general project guidance.

## Most Effective Prompts

- "Implement `src/data_processor.py` to handle data cleaning, transformation, and quality checks. This should include loading raw data, cleaning it, transforming it (e.g., consistent dates, merging, new features), performing quality checks (missing values, outliers, freshness), and saving the processed data. Begin by creating the file and its initial data loading and cleaning structure."
- "Refine `src/data_fetcher.py` to incorporate robust error handling, logging, and rate limiting with exponential backoff, as specified in the project requirements. Also, ensure temperature conversion from tenths of degrees Celsius to Fahrenheit."
- "Create the Streamlit dashboard in `dashboards/app.py` with the four required visualizations: Geographic Overview, Time Series Analysis, Correlation Analysis, and Usage Patterns Heatmap. Include sidebar filters for date range and city selection."
- "Update `pyproject.toml` to include `streamlit`, `plotly`, and `scipy` as dependencies."
- "Create a comprehensive `README.md` file for the project, covering setup, pipeline execution, and dashboard usage."

## AI Mistakes and Fixes

- **Initial File Access Error**: The AI initially attempted to access the project PDF from a restricted directory (`C:\Users\pc\Downloads`).
    - **Fix**: The user was prompted to move the file into the project directory, and the AI then successfully read it.
    - **Learning**: Confirmed the strict sandboxing rules and the necessity for users to place files within the accessible project directory for the AI to interact with them.

- **Empty File Assumption**: The AI initially assumed all Python files in the `src` directory were empty, when `data_fetcher.py` already contained some initial code.
    - **Fix**: The AI performed `read_file` on each file to verify their contents, correcting its assumption.
    - **Learning**: Reinforced the importance of always verifying file contents with `read_file` or `read_many_files` before making assumptions about their state.

## Time Saved Estimate

It is estimated that AI assistance saved approximately **8-10 hours** of development time on this project. This includes time saved on:

- **Boilerplate code generation**: Quickly setting up file structures and initial function definitions.
- **API integration**: Assistance with understanding API documentation and structuring requests.
- **Error handling and logging**: Implementing robust error handling and logging mechanisms.
- **Data transformation logic**: Developing pandas-based data cleaning and transformation steps.
- **Streamlit dashboard development**: Rapid prototyping and implementation of complex visualizations.
- **Documentation generation**: Creating detailed `README.md` and `AI_USAGE.md` files.

This estimate is based on the speed at which complex tasks were initiated and completed with AI guidance, compared to manual development and research.