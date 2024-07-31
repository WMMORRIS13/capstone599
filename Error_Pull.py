import streamlit as st
import pandas as pd
import aiohttp
import asyncio
from sodapy import Socrata
from io import StringIO, BytesIO
from datetime import datetime

# Define your API app token
app_token = "YourAPIkey"
client = Socrata("data.wa.gov", app_token)

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.json()

def format_date(date_str):
    # Convert timestamp to readable date format if necessary
    try:
        date = datetime.utcfromtimestamp(int(date_str))
        return date.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return date_str

async def fetch_dataset_info(url, error):
    async with aiohttp.ClientSession() as session:
        data = await fetch(session, url)

        created_at = format_date(data.get("createdAt", "N/A"))
        updated_at = format_date(data.get("indexUpdatedAt", created_at))  # Use created_at if dataUpdatedAt is missing

        dataset_info = {
            "Unique ID": data.get("id", "N/A"),
            "Dataset Name": data.get("name", "N/A"),
            "Type": data.get("assetType", "N/A"),
            "Initial Upload Date": created_at,
            "Last Update": updated_at,
            "Dataset Owner": data.get("owner", {}).get("displayName", "N/A"),
            "Derived View": data.get("viewType", "N/A"),
            "Parent UID": data.get("parentUid", "N/A"),
            "Dataset link": f"https://data.wa.gov/api/views/metadata/v1/{data.get('id', '')}",
            "error": error
        }
        return dataset_info

async def fetch_all_datasets(urls_with_errors):
    tasks = [fetch_dataset_info(url, error) for url, error in urls_with_errors]
    return await asyncio.gather(*tasks)

st.title("Dataset Info Extractor")

# Display the logo
logo_url = "https://msimonline.ischool.uw.edu/wp-content/uploads/sites/2/2022/09/Screen-Shot-2022-09-28-at-11.44.42-AM-1.png"
st.markdown(
    f"""
    <style>
    .logo {{
        position: fixed;
        bottom: 0;
        right: 0;
        padding: 10px;
    }}
    </style>
    <div class="logo">
        <img src="{logo_url}" width="557">
    </div>
    """,
    unsafe_allow_html=True
)


uploaded_file = st.file_uploader("Upload a TXT file", type="txt")

if uploaded_file is not None:
    content = StringIO(uploaded_file.getvalue().decode("utf-8"))
    lines = content.readlines()

    urls_with_errors = []
    for line in lines:
        if "Identifier:" in line:
            url_start = line.find("Identifier:") + len("Identifier:")
            url_end = line.find("; Title:")
            if url_start != -1 and url_end != -1:
                url = line[url_start:url_end].strip()
                error_start = line.find("Found.") + len("Found.")
                error = line[error_start:].strip()
                urls_with_errors.append((url, error))

    if urls_with_errors:
        with st.spinner("Fetching dataset information..."):
            dataset_infos = asyncio.run(fetch_all_datasets(urls_with_errors))

        results = [info for info in dataset_infos if info is not None]

        if results:
            df = pd.DataFrame(results)
            st.write(df)

            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            excel_buffer.seek(0)

            st.download_button(
                label="Download Excel File",
                data=excel_buffer,
                file_name="dataset_info.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
