import concurrent.futures
import json
import logging

import pandas as pd
import requests
from airflow.models import Variable
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from rapidfuzz import fuzz, process

logging.basicConfig(level=logging.INFO)

# API Key
api_key = Variable.get("api_key")

# URLs
unirank_url = "https://www.4icu.org/us/"
scorecard_url = "https://api.data.gov/ed/collegescorecard/v1/schools"
agent = UserAgent().random
params = {
    "api_key": api_key,
    "per_page": 95,
    "page": 0
}


# STEP 1: SCRAPE TOP 1000 UNIVERSITIES
def scrape_universities():
    """Scrape the top 1,000 universities from uniRank."""
    try:
        response = requests.get(unirank_url, headers={"User-Agent": agent})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        # Find the university list in the table
        uni_table = soup.find("table", {"class": "table"})
        universities = []

        if uni_table:
            rows = uni_table.find_all("tr")[1:] 
            for rank, row in enumerate(rows[:1000], start=1):
                columns = row.find_all("td")
                if columns:
                    uni_name = columns[1].text.strip()
                    location = columns[2].text.strip() 
                    universities.append((rank, uni_name, location))
        uni = pd.DataFrame(universities,
                           columns=["Rank", "University", "Location"])
        uni.to_csv("/usr/local/airflow/dags/universities.csv", index=False)
        logging.info("Scraped and saved university data successfully.")
        return universities
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


# STEP 2: FETCH ALL SCHOOLS FROM SCORECARD API
def fetch_page(page):
    """Fetch school details from the College Scorecard API."""
    try:
        logging.info(f"Fetching page {page}...")
        param = params.copy()
        param["page"] = page
        response = requests.get(scorecard_url, params=param, timeout=10)

        if response.status_code != 200:
            print(f"Error on page {page}: {response.status_code}")
            return []

        data = response.json()
        results_data = data["results"]
        return results_data

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def concurrent_fetch_pages():
    """Fetch school details concurrently using ThreadPoolExecutor."""
    # Use ThreadPoolExecutor to fetch multiple pages concurrently
    all_schools = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(fetch_page, range(0, 69)))

    # Flatten results and extract school data
    try:
        for schools in results:
            for school in schools:
                school_data = {
                    "ID": school.get("id"),
                    "name": school.get("school", {}).get("name", ""),
                    "city": school.get("school", {}).get("city", ""),
                    "State": school.get("latest", {}).get("school", {}).get(
                        "state", ""),
                    "Region ID": school.get("latest", {}).get(
                        "school", {}).get("region_id", ""),
                    "Zip Code": school.get("latest", {}).get(
                        "school", {}).get("zip", ""),
                    "Locale": school.get("latest", {}).get(
                        "school", {}).get("locale", ""),
                    "State FIPS": school.get("latest", {}).get(
                        "school", {}).get("state_fips", ""),
                    "Ownership": school.get("latest", {}).get(
                        "school", {}).get("ownership", ""),
                    "School URL": school.get("latest", {}).get(
                        "school", {}).get("school_url", ""),
                    "Operating": school.get("latest", {}).get(
                        "school", {}).get("operating", ""),
                    "Accreditor": school.get("latest", {}).get(
                        "school", {}).get("accreditor", ""),
                    "Online Only": school.get("latest", {}).get(
                        "school", {}).get("online_only", ""),
                    "Main Campus": school.get("latest", {}).get(
                        "school", {}).get("main_campus", ""),
                    "Branches": school.get("latest", {}).get(
                        "school", {}).get("branches", ""),
                    "Men Only": school.get("latest", {}).get(
                        "school", {}).get("men_only", ""),
                    "Women Only": school.get("latest", {}).get(
                        "school", {}).get("women_only", ""),
                    "Institution Level": school.get("latest", {}).get(
                        "school", {}).get(
                            "institutional_characteristics", {}).get(
                                "level", ""),
                    "Open Admissions Policy": school.get("latest", {}).get(
                        "school", {}).get("open_admissions_policy", ""),
                    "Latitude": school.get("location", {}).get("lat", ""),
                    "Longitude": school.get("location", {}).get("lon", ""),
                    "Highest Degree Awarded": school.get("latest", {}).get(
                        "school", {}).get("degrees_awarded", {}).get(
                            "highest", ""),
                    "Carnegie Basic": school.get("latest", {}).get(
                        "school", {}).get("carnegie_basic", ""),
                    "Carnegie Undergrad": school.get("latest", {}).get(
                        "school", {}).get("carnegie_undergrad", ""),
                    "Ownership PEPS": school.get("latest", {}).get(
                        "school", {}).get("ownership_peps", ""),
                    "PEPS Ownership": school.get("latest", {}).get(
                        "school", {}).get("peps_ownership", ""),
                    "Test Requirements": school.get("latest", {}).get(
                        "admissions", {}).get("test_requirements", ""),
                    "Admission Rate": school.get("latest", {}).get(
                        "admissions", {}).get("admission_rate", {}).get(
                            "overall", ""),
                    "SAT Overall Average": school.get("latest", {}).get(
                        "admissions", {}).get("sat_scores", {}).get(
                            "average", {}).get("overall", ""),
                    "SAT Reading": school.get("latest", {}).get(
                        "admissions", {}).get("sat_scores", {}).get(
                            "50th_percentile", {}).get("critical_reading", ""),
                    "SAT Math": school.get("latest", {}).get(
                        "admissions", {}).get("sat_scores", {}).get(
                            "50th_percentile", {}).get("math", ""),
                    "ACT Cumulative": school.get("latest", {}).get(
                        "admissions", {}).get("act_scores", {}).get(
                            "50th_percentile", {}).get("cumulative", ""),
                    "ACT English": school.get("latest", {}).get(
                        "admissions", {}).get("act_scores", {}).get(
                            "50th_percentile", {}).get("english", ""),
                    "ACT Math": school.get("latest", {}).get(
                        "admissions", {}).get("act_scores", {}).get(
                            "50th_percentile", {}).get("math", ""),
                    "Loan Principal": school.get("latest", {}).get(
                        "aid", {}).get("loan_principal", ""),
                    "Pell Grant Rate": school.get("latest", {}).get(
                        "aid", {}).get("pell_grant_rate", ""),
                    "Federal Loan Rate": school.get("latest", {}).get(
                        "aid", {}).get("federal_loan_rate", ""),
                    "Median Debt Dependent": school.get("latest", {}).get(
                        "aid", {}).get("median_debt", {}).get(
                            "dependent_students", ""),
                    "Median Debt Independent": school.get("latest", {}).get(
                        "aid", {}).get("median_debt", {}).get(
                            "independent_students", ""),
                    "Tuition In-State": school.get("latest", {}).get(
                        "cost", {}).get("tuition", {}).get("in_state", ""),
                    "Tuition Out-State": school.get("latest", {}).get(
                        "cost", {}).get("tuition", {}).get("out_of_state", ""),
                    "Book Supply": school.get("latest", {}).get(
                        "cost", {}).get("booksupply", ""),
                    "Room & Board On-campus": school.get("latest", {}).get(
                        "cost", {}).get("roomboard", {}).get("oncampus", ""),
                    "Room & Board Off-campus": school.get("latest", {}).get(
                        "cost", {}).get("roomboard", {}).get("offcampus", ""),
                    "Other Expense On-campus": school.get("latest", {}).get(
                        "cost", {}).get("otherexpense", {}).get(
                            "oncampus", ""),
                    "Other Expense Off-campus": school.get("latest", {}).get(
                        "cost", {}).get("otherexpense", {}).get(
                            "offcampus", ""),
                    "Other Expense With Family": school.get("latest", {}).get(
                        "cost", {}).get("otherexpense", {}).get(
                            "withfamily", ""),
                    "Avg Net Price": school.get("latest", {}).get(
                        "cost", {}).get("avg_net_price", {}).get(
                            "over_all", ""),
                    "Student Size": school.get("latest", {}).get(
                        "student", {}).get("size", ""),
                    "Grad Students": school.get("latest", {}).get(
                        "student", {}).get("grad_students", ""),
                    "Men Demographics": school.get("latest", {}).get(
                        "student", {}).get("demographics", {}).get("men", ""),
                    "Women Demographics": school.get("latest", {}).get(
                        "student", {}).get("demographics", {}).get(
                            "women", ""),
                    "Age Entry": school.get("latest", {}).get(
                        "student", {}).get("demographics", {}).get(
                            "age_entry", ""),
                    "Faculty Men": school.get("latest", {}).get(
                        "student", {}).get("demographics", {}).get(
                            "faculty", {}).get("men", ""),
                    "Faculty Women": school.get("latest", {}).get(
                        "student", {}).get("demographics", {}).get(
                            "faculty", {}).get("women", ""),
                    "Enrollment Grad 12-month": school.get("latest", {}).get(
                        "student", {}).get("enrollment", {}).get(
                            "grad_12_month", ""),
                    "Enrollment Undergrad 12-month": school.get(
                        "latest", {}).get("student", {}).get(
                            "enrollment", {}).get("undergrad_12_month", ""),
                    "Retention Rate - Overall Full-Time": school.get(
                        "latest", {}).get("student", {}).get(
                            "retention_rate", {}).get("overall", {}).get(
                                "full_time", ""),
                    "Retention 4-year Full-time": school.get(
                        "latest", {}).get("student", {}).get(
                            "retention_rate", {}).get("four_year", {}).get(
                                "full_time", ""),
                    "Retention 4-year Part-time": school.get(
                        "latest", {}).get("student", {}).get(
                            "retention_rate", {}).get("four_year", {}).get(
                                "part_time", ""),
                    }
                all_schools.append(school_data)

        # Save as JSON file
        with open("/usr/local/airflow/dags/schools_data.json", "w") as f:
            json.dump(all_schools, f, indent=4)

        print("Data successfully saved as JSON!")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def match_universities():
    """Match scraped universities with API results using fuzzy matching."""
    try:
        logging.info("Matching universities...")
        with open("/usr/local/airflow/dags/schools_data.json", 'r') as f:
            data = json.load(f)
        df = pd.json_normalize(data)
        uni = pd.read_csv("/usr/local/airflow/dags/universities.csv")

        matched_results = []
        # Get school names from the 'name' column of the DataFrame
        api_school_names = df['name'].tolist()

        for rank, scraped_name, location in uni.itertuples(index=False):
            match, score, index = process.extractOne(
                scraped_name, api_school_names, scorer=fuzz.WRatio)

            if score >= 85:  # Adjust threshold as needed
                # Get the row from the DataFrame using iloc
                matched_results.append([rank, scraped_name,
                                        location, match, score] + list(
                                            df.iloc[index].values))
            else:
                matched_results.append([rank, scraped_name,
                                        location, "No Match",
                                        score] + [""] * len(df.columns))
        matched_results = pd.DataFrame(matched_results,
                                       columns=["Rank", "Scraped Name",
                                                "Location", "Match",
                                                "Score"] + df.columns.tolist())
        columns_to_drop = ["Location", "Match"]
        mat = matched_results.drop(columns=columns_to_drop)
        final_mat = mat[mat["Score"] > 80]
        final_mat.to_csv("/usr/local/airflow/dags/matched_universities.csv",
                         index=False)
        logging.info("Matching completed successfully.")
        return final_mat
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
