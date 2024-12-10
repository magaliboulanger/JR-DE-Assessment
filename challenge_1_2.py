# -*- coding: utf-8 -*-
import pandas as pd
import requests
from configparser import ConfigParser
from requests.structures import CaseInsensitiveDict

bronze_path = 'data/bronze/'
products_path = f"{bronze_path}/products.csv"
orders_path = f"{bronze_path}/orders.csv"
full_orders_path = f"{bronze_path}/order_full_information.csv"

def load_data():
    products_df = pd.read_csv(products_path)
    orders_df = pd.read_csv(orders_path)
    return products_df, orders_df

def consolidate_orders():
    products_df,orders_df = load_data()
    # Join the tables with an inner join, keeping the columns we need and renaming them.
    full_orders_information = pd.merge(orders_df, products_df, left_on='product_id', right_on='id')[
        ['created_date', 'id_x', 'name', 'quantity', 'price']].rename(columns={
        'created_date': 'order_created_date',
        'id_x': 'order_id',
        'name': 'product_name',
        'price': 'total_price'
    })
    # Update the price according to the quantity of products
    full_orders_information['total_price'] = full_orders_information['quantity']*full_orders_information['total_price']
    # Save the data in a new cvs file
    full_orders_information.to_csv(full_orders_path, index=False)
    return full_orders_information

print("Challenge 1")
full_orders_df = consolidate_orders()
print(full_orders_df)


def obtain_data_exchange(base_url, endpoint, parameters = {}):
    # Obtain the key from the config file
    parser = ConfigParser()
    parser.read("pipeline.conf")
    api_credentials = parser["api-credentials"]
    api_key = api_credentials["api_key"]

    # Header information for the request
    headers = CaseInsensitiveDict()
    headers["apikey"] = api_key
   
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()  # Raise an exception if there is an error in the response HTTP.

        # Verify if the data is in JSON fromat.
        try:
            data = response.json()
        except:
            print("The response format is not as expected")
            return None
        return data

    except requests.exceptions.RequestException as e:
        print(f"The request has failed. Error code : {e}")
        return None


def update_currency_data(orders, brl_usd_exch, path):
    # Update the currency 
    full_orders_fixed_df = orders
    full_orders_fixed_df['total_price_us'] = full_orders_fixed_df['total_price']*brl_usd_exch
    full_orders_fixed_df = full_orders_fixed_df.rename( columns = {'total_price':'total_price_br'})

    full_orders_fixed_df.to_csv(path, index=False)
    return full_orders_fixed_df


def calculate_kpis(orders, kpi_path):

    #1. Date where we create the max amount of orders.
    """
    To obtain this data, it is necessary to group the data by the date of the order, and then count the amount of records (orders) for each day. 
    After that, we ask for the max amount of order count and the associated day. 
    In case there are more than one day with the same number of orders, all of them are selected.
    """
    orders_count_by_date = (
        orders.groupby('order_created_date')
        .agg(orders_count=('order_id', 'count'))
        .reset_index()
    )

    max_quantity = orders_count_by_date['orders_count'].max()
    all_max_rows = orders_count_by_date[orders_count_by_date['orders_count'] == max_quantity]

    #2. Most demanded product and the total sell price.
    """
    To obtain this data I'll assume the product name is a unique field as rule. 
    In case, this is not true, we'll need to rejoin the data considering the product_id field to identify any product correctly.
    """
    most_demanded_prod = (
        orders.groupby('product_name')
        .agg(total_quantity=('quantity', 'sum'), total_price_us=('total_price_us', 'sum'))
        .reset_index()
    )
    most_demanded_prod = most_demanded_prod[most_demanded_prod['total_quantity'].max()==most_demanded_prod['total_quantity']]

    #3. The top 3 most demanded categories.
    """
    To obtain this data, is necessary to rejoin the data because the categories are not included in the fixed currency dataframe. 
    After that, we group the data considering the category, and sum the amount of products selled. 
    Finally, the categories were sorted in descending order and print the first 3 rows.
    """
    products_df,orders_df = load_data()
    most_demanded_categ = pd.merge(orders_df, products_df, left_on='product_id', right_on='id', how='left')
    most_demanded_categ = most_demanded_categ.groupby('category').agg(total_quantity=('quantity', 'sum'))
    top_3_categ = most_demanded_categ.sort_values(by='total_quantity', ascending=False).head(3)
    top_3_categ_names = top_3_categ.index.tolist()

    # Save KPIs to CSV
    kpi_data = {
    "kpi": ["Max Orders Date", "Most Demanded Product", "Top 3 Categories"],
    "value": [all_max_rows['order_created_date'].tolist(), most_demanded_prod['product_name'].tolist() + most_demanded_prod['total_price_us'].tolist(), top_3_categ_names]
    }
    kpi_df = pd.DataFrame(kpi_data)
    kpi_df.to_csv(kpi_path, index=False)
    return kpi_df

print("Challenge 2")
# Parameters needed for this API request
params = {
    "base_currency": 'BRL',
    "currencies": 'USD'
    }
api_url = "https://api.freecurrencyapi.com/v1/"
endpoint = "latest"
brl_usd_exchange = obtain_data_exchange(api_url, endpoint, params)['data']['USD']
fixed_data_path = f"{bronze_path}/fixed_order_full_information.csv"
kpi_path = f"{bronze_path}/kpi_product_orders.csv"

print("The BRL-USD exchange is:", brl_usd_exchange)

print(update_currency_data(full_orders_df, brl_usd_exchange, fixed_data_path))

print(calculate_kpis(full_orders_df, kpi_path))