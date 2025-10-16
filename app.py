import streamlit as st
from PIL import Image

# --- Page configuration ---
st.set_page_config(
    page_title="Zoho Data Project",
    layout="wide"
)

# --- Custom CSS ---
st.markdown("""
    <style>
    .sidebar .sidebar-content {
        background-color: #f0f2f6;
        padding: 20px;
    }

    .sidebar h3 {
        color: #2c3e50;
        font-weight: 700;
        font-size: 20px;
    }

    h1, h2, h3 {
        color: #1f4068;
    }

    .stCodeBlock {
        background-color: #f7f7f7;
        border-left: 4px solid #1f4068;
        border-radius: 5px;
        padding: 10px;
    }
    </style>
""", unsafe_allow_html=True)

# --- Sidebar with Logo and Navigation ---
with st.sidebar:
    logo = Image.open("de.png").resize((128, 128))
    st.image(logo, use_container_width=False)

    st.markdown("<h3 style='text-align:left; margin-top:10px;'>Project Sections</h3>", unsafe_allow_html=True)
    section = st.radio(
        "",
        ["Introduction", "Source Code", "ETL Pipeline", "SQL Script", "Final Report", "Confluence  Wiki"],
        label_visibility="collapsed"
    )

# --- Pages ---
if section == "Introduction":
    st.title("📘 Zoho Data Integration Project")
    st.write("""
    Welcome to the **Zoho Data Project Showcase** — a data engineering and analytics initiative that demonstrates 
    how raw data from **Zoho Recruit** can be extracted, transformed, and prepared for reporting using enterprise tools.

    ### 🧭 Project Overview
    The goal of this project is to:
    - **Integrate Zoho Recruit data** into an enterprise data warehouse.
    - Build an **ETL pipeline** using **WhereScape** and **Azure Data Factory (ADF)**.
    - Generate **SQL transformations** to shape data for analytics.
    - Produce a **final Power BI report** for business stakeholders.
    - Document all processes in **Confluence DevOps Wiki** for transparency and traceability.

    ### 🛠 Tech Stack
    - **Python**: Data extraction from Zoho API (`zoho.py`)
    - **WhereScape / Azure Data Factory**: ETL orchestration
    - **SQL Server**: Data modeling and transformation
    - **Power BI**: Reporting & visualization
    - **Azure DevOps Wiki**: Team documentation

    ### 📁 Project Structure
    - **Source Code** → Python script to pull data from Zoho Recruit API  
    - **ETL Pipeline** → Data ingestion & transformation workflow  
    - **SQL Script** → SQL transformations for analytics  
    - **Final Report** → Visualization of candidate insights  
    - **Confluence  Wiki** → Documentation snapshot  

    ---
    Select a section from the sidebar to explore each component of the project.
    """)

    st.image("overview.png", caption="High-level Data Flow Overview", use_container_width=True)

elif section == "Source Code":
    st.header("🖥 Source Python Code: zoho.py")
    st.write("This script demonstrates **data extraction logic** from Zoho Recruit API. Credentials are removed for privacy.")
    with open("zoho.py", "r") as f:
        st.code(f.read(), language="python")

elif section == "ETL Pipeline":
    st.header("⚙️ ETL Pipeline (WhereScape / ADF)")
    st.write("""
This is the **WhereScape and ADF ETL pipeline** for Zoho candidate data.  
It shows how raw API data is transformed and prepared for reporting.

**Workflow:**
- `[V_api_zohoadmin_candidates]` → Source view from the Zoho API.  
- `[stage_api_zohoadmin_candidates]` → Staging layer for initial cleansing and validation.  
- `[dl_api_zohoadmin_candidates]` → Data layer for structured storage and integration.  
- Final views:  
  - `[V_dl_api_zohoadmin_candidates_current]` → Current snapshot of processed candidates.  
  - `[V_dlbv_api_zohoadmin_candidates_current]` → Reporting-ready view for analytics.

This flow ensures **data consistency, traceability, and readiness** for reporting, while clearly separating **raw ingestion**, **staging**, and **final reporting layers**.
""")
    st.image("etl_zoho.png", use_container_width=True)

elif section == "SQL Script":
    st.header("📄 SQL Script: _zoho.sql")
    st.write("Transforms Zoho candidate & call data into **report-ready datasets** with key metrics.")
    with open("_zoho.sql", "r") as f:
        st.code(f.read(), language="sql")

elif section == "Final Report":
    st.header("📊 Final Report")
    st.write("Screenshot of the final report showing **metrics for talent acquisition**.")
    st.image("report.png", use_container_width=True)

elif section == "Confluence  Wiki":
    st.header("📚 Confluence  Wiki Documentation")
    st.write("Preview of **team documentation** for transparency, ownership, and traceability.")
    st.image("wiki_doc.png", caption="Sample Confluence  Wiki Page", use_container_width=True)

# --- Footer ---
st.markdown("---")
st.markdown("<p style='text-align:center; color:gray;'>Project showcase created with Streamlit — all code and data anonymized.</p>", unsafe_allow_html=True)
