import streamlit as st
import pandas as pd
import aiohttp
import asyncio
from sodapy import Socrata
from io import StringIO, BytesIO
from datetime import datetime, timezone
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import time

# Define your API app token
app_token = "API Token"

# Initialize session state if not already present
if 'state_abbr' not in st.session_state:
    st.session_state.state_abbr = ''
if 'email' not in st.session_state:
    st.session_state.email = ''
if 'password' not in st.session_state:
    st.session_state.password = ''

# Streamlit app setup
st.title("Dataset Info Extractor")

# Sidebar login
st.sidebar.title("Login")
state = st.sidebar.selectbox(
    "Select your state",
    options=[
        ("Alabama", "al"), ("Alaska", "ak"), ("Arizona", "az"), ("Arkansas", "ar"),
        ("California", "ca"), ("Colorado", "co"), ("Connecticut", "ct"), ("Delaware", "de"),
        ("Florida", "fl"), ("Georgia", "ga"), ("Hawaii", "hi"), ("Idaho", "id"),
        ("Illinois", "il"), ("Indiana", "in"), ("Iowa", "ia"), ("Kansas", "ks"),
        ("Kentucky", "ky"), ("Louisiana", "la"), ("Maine", "me"), ("Maryland", "md"),
        ("Massachusetts", "ma"), ("Michigan", "mi"), ("Minnesota", "mn"), ("Mississippi", "ms"),
        ("Missouri", "mo"), ("Montana", "mt"), ("Nebraska", "ne"), ("Nevada", "nv"),
        ("New Hampshire", "nh"), ("New Jersey", "nj"), ("New Mexico", "nm"), ("New York", "ny"),
        ("North Carolina", "nc"), ("North Dakota", "nd"), ("Ohio", "oh"), ("Oklahoma", "ok"),
        ("Oregon", "or"), ("Pennsylvania", "pa"), ("Rhode Island", "ri"), ("South Carolina", "sc"),
        ("South Dakota", "sd"), ("Tennessee", "tn"), ("Texas", "tx"), ("Utah", "ut"),
        ("Vermont", "vt"), ("Virginia", "va"), ("Washington", "wa"), ("West Virginia", "wv"),
        ("Wisconsin", "wi"), ("Wyoming", "wy")
    ]
)
st.session_state.state_abbr = state[1]

email = st.sidebar.text_input("Email")
password = st.sidebar.text_input("Password", type="password")

if st.sidebar.button("Login"):
    # Store credentials for later use
    st.session_state.email = email
    st.session_state.password = password
    st.sidebar.success("Credentials stored. Use them in the script.")

# Function to dynamically generate the base URL based on the state abbreviation
def get_base_url(state_abbr):
    return f"https://data.{state_abbr}.gov/api/views/metadata/v1/"

# Function to perform login using Selenium
def login(state_abbr, email, password):
    options = Options()
    options.headless = True  # Change to False if you want the window to pop up
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        # Navigate to login page
        login_url = f"https://data.{state_abbr}.gov/login"
        driver.get(login_url)

        # Fill in email
        email_field = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@name='user_session[login]']"))
        )
        email_field.send_keys(email)

        # Fill in password
        password_field = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@name='user_session[password]']"))
        )
        password_field.send_keys(password)

        # Click Sign In button
        sign_in_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@class='btn-content' and text()='Sign In']"))
        )
        sign_in_button.click()

        # Check for login success
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//div[@class='navbar-text']"))
        )
        st.sidebar.success("Login successful!")
        return driver

    except Exception as e:
        st.sidebar.error("Login failed. Please check your credentials.")
        return None

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.json()

def format_date(date_str):
    try:
        timestamp = int(date_str)
        date = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return date.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return date_str

async def fetch_dataset_info(url, error):
    async with aiohttp.ClientSession() as session:
        data = await fetch(session, url)

        created_at = format_date(data.get("createdAt", "N/A"))
        updated_at = format_date(data.get("indexUpdatedAt", created_at))  # Use created_at if indexUpdatedAt is missing

        dataset_info = {
            "Unique ID": data.get("id", "N/A"),
            "Dataset Name": data.get("name", "N/A"),
            "Type": data.get("assetType", "N/A"),
            "Initial Upload Date": created_at,
            "Last Update": updated_at,
            "Dataset Owner": data.get("owner", {}).get("displayName", "N/A"),
            "Derived View": data.get("viewType", "N/A"),
            "Parent UID": data.get("parent_fxf", "N/A"),
            "Dataset link": f"{get_base_url(st.session_state.state_abbr)}{data.get('id', '')}",
            "error": error
        }
        return dataset_info

async def fetch_all_datasets(urls_with_errors):
    tasks = [fetch_dataset_info(url, error) for url, error in urls_with_errors]
    return await asyncio.gather(*tasks)

# Update description function using Selenium
def update_description(dataset_id, new_description):
    if not st.session_state.email or not st.session_state.password:
        st.sidebar.error("Please login first.")
        return

    options = Options()
    options.headless = True  # Change to False if you want the window to pop up
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        # Navigate to login page
        login_url = f"https://data.{st.session_state.state_abbr}.gov/login"
        driver.get(login_url)

        # Fill in email
        email_field = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@name='user_session[login]']"))
        )
        email_field.send_keys(st.session_state.email)

        # Fill in password
        password_field = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@name='user_session[password]']"))
        )
        password_field.send_keys(st.session_state.password)

        # Click Sign In button
        sign_in_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@class='btn-content' and text()='Sign In']"))
        )
        sign_in_button.click()

        # Navigate to browse page
        driver.get(f"https://data.{st.session_state.state_abbr}.gov/browse")

        # Search for the dataset by ID
        search_field = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//input[@aria-label='Search']"))
        )
        search_field.send_keys(dataset_id)
        search_field.send_keys(u'\ue007')  # Press Enter key

        # Wait for search results and click on the dataset title
        dataset_title = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//a[contains(@href, '{dataset_id}')]"))
        )
        dataset_title.click()

        # Click on "Edit"
        edit_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//span[@class='forge-button__ripple']"))
        )
        edit_button.click()

        # Click "Edit Metadata"
        edit_metadata_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@class='btn-content']"))
        )
        edit_metadata_button.click()

        # Update the description
        description_textbox = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//textarea[@name='Brief description']"))
        )
        description_textbox.clear()
        description_textbox.send_keys(new_description)

        # Save changes
        save_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Save')]"))
        )
        save_button.click()

        # Wait for 5 seconds
        time.sleep(5)

        # Confirm update
        update_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//span[@class='forge-button__ripple']"))
        )
        update_button.click()

        # Post update
        post_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'btn btn-primary continue-button false')]"))
        )
        post_button.click()

    except Exception as e:
        # Save a screenshot for debugging
        screenshot_path = f"screenshots/error_{dataset_id}.png"
        driver.save_screenshot(screenshot_path)
        st.error(f"An error occurred while updating dataset {dataset_id}. Screenshot saved to {screenshot_path}.")
        st.error(f"Exception: {e}")

    finally:
        driver.quit()

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

            # Display checkboxes for each dataset
            selected_datasets = []
            for index, row in df.iterrows():
                selected = st.checkbox(
                    f"{row['Dataset Name']} - {row['Type']} - {row['Last Update']} - {row['Dataset Owner']} - {row['Derived View']} - {row['Parent UID']}",
                    key=row['Unique ID']
                )
                if selected:
                    selected_datasets.append(row)

            if selected_datasets:
                # Display preview of selected datasets
                st.write("Dataset Preview:")
                for dataset in selected_datasets:
                    st.write(pd.DataFrame(dataset).T)

                # User input for description update
                new_description = st.text_area("Enter the new description for selected datasets")

                if st.button("Update Selected Datasets"):
                    for dataset in selected_datasets:
                        update_description(dataset["Unique ID"], new_description)
                        st.success(f"Updated dataset {dataset['Unique ID']}.")

            # Prepare Excel file for download
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Datasets')
            excel_buffer.seek(0)

            st.download_button(
                label="Download Dataset Preview",
                data=excel_buffer,
                file_name="dataset_info.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )