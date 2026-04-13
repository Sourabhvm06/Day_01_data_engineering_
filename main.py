import requests
import pandas as pd
import logging


#set up logging
logging.basicConfig(filename='app.log', level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
try:
    #EXTRACT (API)
    url="https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("API request failed")

    data = response.json()
    logging.info("Data fetched successfully")

    # 2. PROCESS
    df = pd.DataFrame(data)

    # Select only required columns
    df = df[["userId", "id", "title"]]

    # Rename columns
    df.columns = ["user_id", "post_id", "title"]

    logging.info("Data processed")

    # 3. STORE
    df.to_csv("output.csv", index=False)
    df.to_parquet("output.parquet")

    logging.info("Data saved successfully")

except Exception as e:
    logging.error(f"Pipeline failed: {e}")
    print("Error occurred:", e)