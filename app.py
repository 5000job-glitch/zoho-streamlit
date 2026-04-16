from pathlib import Path

import streamlit as st


st.set_page_config(page_title="Azure Data Factory Project", layout="wide")


IMAGE_DIR = Path(__file__).resolve().parent / "images" / "adf"


def image_path(filename: str) -> Path:
    return IMAGE_DIR / filename


st.markdown(
    """
    <style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: none;
        padding-left: 5rem;
        padding-right: 2rem;
    }

    [data-testid="stAppViewContainer"] > .main {
        padding-left: 0;
    }

    [data-testid="stHorizontalBlock"] {
        gap: 0.75rem;
    }

    [data-testid="stSidebar"] {
        background: #eef2f7;
        border-right: 1px solid #d9e2ef;
    }

    [data-testid="stSidebar"] .stRadio label {
        color: #244a7d;
        font-weight: 500;
    }

    [data-testid="stSidebar"] h1 {
        color: #1f4c87;
    }

    .sidebar-logo {
        font-size: 4.2rem;
        text-align: left;
        line-height: 1;
        margin: 0.5rem 0 1.75rem;
        color: #1c78d1;
    }

    .page-subtitle {
        color: #2f4158;
        font-size: 1rem;
        margin-bottom: 1.15rem;
        max-width: 980px;
    }

    .section-card {
        background: #f6f8fc;
        border: 1px solid #d9e2ef;
        border-radius: 12px;
        padding: 0.9rem 1rem;
        margin-bottom: 1.1rem;
        max-width: 1040px;
    }

    .section-heading {
        color: #1f4c87;
        font-size: 1.05rem;
        font-weight: 700;
        margin: 0;
    }

    .section-text {
        color: #2f4158;
        margin: 0.5rem 0 0;
        line-height: 1.6;
    }

    .image-frame {
        margin-top: 0.35rem;
        margin-bottom: 0.5rem;
        max-width: 1040px;
    }

    .section-label {
        color: #1f4c87;
        font-size: 1.35rem;
        font-weight: 700;
        margin: 1rem 0 0.75rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def show_image(filename: str, caption: str, width: int = 7) -> None:
    columns = st.columns([width, max(12 - width, 1)])
    with columns[0]:
        st.markdown('<div class="image-frame">', unsafe_allow_html=True)
        st.image(image_path(filename), use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        st.caption(caption)


def render_title(title: str, icon: str, subtitle: str) -> None:
    st.title(f"{icon} {title}")
    if subtitle:
        st.markdown(
            f'<div class="page-subtitle">{subtitle}</div>',
        unsafe_allow_html=True,
    )


def render_section_label(label: str) -> None:
    st.markdown(
        f'<div class="section-label">{label}</div>',
        unsafe_allow_html=True,
    )


def render_note(title: str, text: str) -> None:
    st.markdown(
        f"""
        <div class="section-card">
            <p class="section-heading">{title}</p>
            <p class="section-text">{text}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_intro() -> None:
    render_title(
        "Azure Data Factory Overview",
        "📘",
        "",
    )
    render_section_label("🧭 Project Overview")
    st.write("The goal of this project is to:")
    st.markdown(
        """
        - **Integrate source data** into a structured Azure Data Factory workflow.
        - Build an **ADF pipeline** using reusable orchestration and parameterization patterns.
        - Generate **data flow transformations** to shape data for downstream use.
        - Apply **metadata checks** and **conditional branching** for execution control.
        - Support delivery through **Git / pull request workflow** for maintainability and change management.
        """
    )

    render_section_label("⭐ Key Highlights")
    st.markdown(
        """
        - Variable-driven pipeline design
        - Dynamic dataset parameterization
        - Metadata checks before downstream processing
        - Conditional branching for file handling and control flow
        - Data flow transformation logic
        - Change management through Git / pull request workflow
        """
    )


def render_pipeline_overview() -> None:
    render_title(
        "Main Pipeline Overview",
        "🗺️",
        "End-to-end ADF orchestration flow with setup, movement, validation, and branching activities.",
    )
    render_note(
        "Pipeline Flow",
        "The main pipeline is built as a reusable orchestration layer that handles setup, movement, validation, and routing decisions in a clear sequence.",
    )
    show_image(
        "adf_project.png",
        "Main pipeline overview showing variable setup, cleanup, source query, copy activity, data flow, metadata check, and conditional branching.",
        width=10,
    )


def render_parameterization() -> None:
    render_title(
        "Parameterized Dataset Design",
        "⚙️",
        "Dataset paths and filenames are driven dynamically instead of being hardcoded.",
    )
    render_note(
        "Reusable Dataset Pattern",
        "Datasets are parameterized so the same pattern can be reused across folders, files, and execution contexts.",
    )
    show_image(
        "image (1).png",
        "Parameterized dataset configuration using dynamic directory and filename variables.",
        width=8,
    )


def render_conditional_logic() -> None:
    render_title(
        "Conditional Logic And Control",
        "🔀",
        "Metadata checks and branching decisions are used to control downstream execution.",
    )
    render_note(
        "Metadata-Driven Control",
        "Metadata is checked before downstream processing so the workflow can continue, branch, or stop based on pipeline state.",
    )
    show_image(
        "image (2).png",
        "Conditional logic used to check metadata and control the next processing steps.",
        width=8,
    )


def render_supporting_details() -> None:
    render_title(
        "Additional Implementation Details",
        "📎",
        "Supporting evidence from expressions, branching, delivery workflow, and output storage.",
    )

    st.write(
        "These screenshots highlight the implementation details behind the main ADF design."
    )

    details = [
        ("image (3).png", "Dynamic content / expression example."),
        ("image (4).png", "Branching and integration workflow context."),
        ("image (5).png", "Pull request example showing deployment/change management."),
        ("image (6).png", "Output landing in storage."),
    ]

    for left, right in zip(details[::2], details[1::2]):
        columns = st.columns([1, 1], gap="large")
        for column, (filename, caption) in zip(columns, [left, right]):
            with column:
                st.markdown('<div class="image-frame">', unsafe_allow_html=True)
                st.image(image_path(filename), use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
                st.caption(caption)


sections = {
    "Introduction": render_intro,
    "Main Pipeline": render_pipeline_overview,
    "Dataset Design": render_parameterization,
    "Conditional Logic": render_conditional_logic,
    "Supporting Details": render_supporting_details,
}


with st.sidebar:
    st.markdown('<div class="sidebar-logo">🗄️⚙️</div>', unsafe_allow_html=True)
    st.title("Project Sections")
    selected_section = st.radio(
        "Navigate the showcase",
        list(sections.keys()),
        label_visibility="collapsed",
    )


sections[selected_section]()
