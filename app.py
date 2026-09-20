import streamlit as st
from pathlib import Path
st.set_page_config(page_icon="👊", page_title="UFC Stats Explorer v1.0", layout="wide",initial_sidebar_state='collapsed')

st.html(Path("assets/style.css"))

pg = st.navigation(
  [st.Page("pages/fdata.py")]
)

pg.run()
