import requests
import statistics
import math
import pandas as pd
from datetime import datetime, timedelta

# Fetch all deals from Pipedrive
def get_all_deals(api_token, base_url="https://api.pipedrive.com/v1"):
    deals = []
    limit = 100
    start = 0
    has_more = True

    while has_more:
        response = requests.get(
            f"{base_url}/deals",
            params={
                "start": start,
                "limit": limit,
                "api_token": api_token
            }
        )
        response.raise_for_status()
        data = response.json()

        if "data" in data and data["data"]:
            deals.extend(data["data"])
            start += limit
            has_more = "additional_data" in data and "pagination" in data["additional_data"] and data["additional_data"]["pagination"]["more_items_in_collection"]
        else:
            has_more = False

    return deals

# Utility and calculation functions
def filter_deals_by_date(deals, date):
    return [deal for deal in deals if datetime.strptime(deal['add_time'], '%Y-%m-%d %H:%M:%S').date() <= date.date()]

def number_of_deals_open_on_date(deals, date):
    return len(deals_open_on_date(deals, date))

def deals_open_on_date(deals, date):
    open_deals = []

    # Convert date to datetime.date if it's a datetime.datetime object
    date = date.date() if isinstance(date, datetime) else date

    for deal in deals:
        add_time = datetime.strptime(deal['add_time'], '%Y-%m-%d %H:%M:%S').date()
        
        # If the deal has a close_time, convert it to a date object
        close_time = None
        if 'close_time' in deal and deal['close_time']:
            close_time = datetime.strptime(deal['close_time'], '%Y-%m-%d %H:%M:%S').date()

        # Check if deal was open on the specified date
        if add_time <= date and (close_time is None or close_time > date):
            open_deals.append(deal)

    return open_deals

def total_value_of_deals_open_on_date(deals, date):
    total_value = 0.0
    filtered_deals = deals_open_on_date(deals, date)

    # Convert date to datetime.date if it's a datetime.datetime object
    date = date.date() if isinstance(date, datetime) else date

    for deal in filtered_deals:
        add_time = datetime.strptime(deal['add_time'], '%Y-%m-%d %H:%M:%S').date()
        if add_time <= date:
            total_value += deal['value']

    return total_value

def deals_won_on_date(deals, date):
    won_deals = []
    for deal in deals:
        won_time = deal.get('won_time', None)
        if won_time:
            won_date = datetime.strptime(won_time, '%Y-%m-%d %H:%M:%S').date()
            if won_date == date.date():
                won_deals.append(deal)
    return won_deals

def num_of_deals_won_on_date(deals, date):
    return len(deals_won_on_date(deals,date))

def total_value_of_deals_won_on_date(deals, date):
    won_deals = deals_won_on_date(deals, date)
    return sum(deal['value'] for deal in won_deals)

def deals_lost_on_date(deals, date):
    lost_deals = []
    for deal in deals:
        lost_time = deal.get('lost_time', None)
        if lost_time:
            lost_date = datetime.strptime(lost_time, '%Y-%m-%d %H:%M:%S').date()
            if lost_date == date.date():
                lost_deals.append(deal)
    return lost_deals

def num_of_deals_lost_on_date(deals, date):
    return len(deals_lost_on_date(deals, date))

def total_value_of_deals_lost_on_date(deals, date):
    lost_deals = deals_lost_on_date(deals, date)
    return sum(deal['value'] for deal in lost_deals)


def age_of_oldest_deal_open_on_date(deals, date):
    open_deals = deals_open_on_date(deals,date)
    if not open_deals:
        return 0
    oldest_open_deal_date = min([datetime.strptime(deal['add_time'], '%Y-%m-%d %H:%M:%S') for deal in open_deals])
    return (date - oldest_open_deal_date).days

def total_age_of_deals_open_on_date(deals, date):
    open_deals = deals_open_on_date(deals,date)
    return sum((date - datetime.strptime(deal['add_time'], '%Y-%m-%d %H:%M:%S')).days for deal in open_deals)

def mean_age_of_deals_open_on_date(deals, date):
    open_deals = deals_open_on_date(deals,date)
    if not open_deals:
        return 0
    total_age = sum((date - datetime.strptime(deal['add_time'], '%Y-%m-%d %H:%M:%S')).days for deal in open_deals)
    return total_age / len(open_deals)


def median_age_of_deals_open_on_date(deals, date):
    open_deals = deals_open_on_date(deals,date)
    if not open_deals:
        return 0
    return statistics.median((date - datetime.strptime(deal['add_time'], '%Y-%m-%d %H:%M:%S')).days for deal in open_deals)

def deals_created_on_date(deals, date):
    created_deals = []
    for deal in deals:
        add_time = deal.get('add_time', None)
        if add_time:
            add_date = datetime.strptime(add_time, '%Y-%m-%d %H:%M:%S').date()
            if add_date == date.date():
                created_deals.append(deal)
    return created_deals

def num_of_deals_created_on_date(deals, date):
    return len(deals_created_on_date(deals, date))

def total_value_of_deals_created_on_date(deals, date):
    created_deals = deals_created_on_date(deals, date)
    return sum(deal['value'] for deal in created_deals)

# Main script
if __name__ == "__main__":

    print("Welcome to Pipedrive KPI Miner")
    print("==============================")
    print("")
    api_key = input("Please paste your Pipedrive API key and press enter: ")
    print("")
    print("Retrieving data from Pipedrive")
    all_deals = get_all_deals(api_key)

    print(len(all_deals), "records retrieved")

    print("Calculating KPI data for past 365 days...")
    
    start_date = datetime.now() - timedelta(days=365)
    results = []

    for i in range(365):
        current_date = start_date + timedelta(days=i)
        
        data_for_day = {
            "Date": current_date.date(),
            "Total # Open Deals": number_of_deals_open_on_date(all_deals, current_date),
            "Total £ Open Deals": total_value_of_deals_open_on_date(all_deals, current_date),
            "# Deals Created": num_of_deals_created_on_date(all_deals, current_date),
            "£ Value of Created Deals": total_value_of_deals_created_on_date(all_deals, current_date),
            "# Deals Won": num_of_deals_won_on_date(all_deals, current_date),
            "£ Deals Won": total_value_of_deals_won_on_date(all_deals, current_date),
            "# Lost Deals": num_of_deals_lost_on_date(all_deals, current_date),
            "£ Lost Deals": total_value_of_deals_lost_on_date(all_deals, current_date),
            "Age of Oldest Open Deal": math.ceil(age_of_oldest_deal_open_on_date(all_deals, current_date)),
            "Total Age of Open Deals": math.ceil(total_age_of_deals_open_on_date(all_deals, current_date)),
            "Mean Age of Open Deals": math.ceil(mean_age_of_deals_open_on_date(all_deals, current_date)),
            "Median Age of Open Deals": math.ceil(median_age_of_deals_open_on_date(all_deals, current_date)),
        }
        results.append(data_for_day)

    df = pd.DataFrame(results)

    # Get the current date and time
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Create an Excel filename with the current timestamp
    filename = f"Pipedrive_KPIs_{current_timestamp}.xlsx"

    # Export the DataFrame to an Excel file
    df.to_excel(filename, index=False)

    print("")
    print("Data exported", filename, " - press enter to exit.")
    input("")
