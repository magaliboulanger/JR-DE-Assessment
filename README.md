# JR DE assesment - Magali Boulanger
## Challenge 1
We have two CSV files: one for products and the other for orders with the next structure.
`products.csv`:
* id
* name
* category
* price

`orders.csv`:
* id
* product_id
* quantity
* created_date

Please note that every order can only contain one product type.
We need to combine both files, to consolidate the information in a single CSV file in the following columns:
* order_created_date
* order_id
* product_name
* quantity
* total_price
This data has to be stored in a newly created file named `order_full_information.csv`.

## Challenge 2
Due to an error in one of our systems, the price information from orders.csv came in Brazilian currency (BRL), we need to convert that value to US dollars.
Get the latest currency data from: [https://app.freecurrencyapi.com/](https://app.freecurrencyapi.com/)
You will need to get an API key from the website.

Include a step in your code to get this information, and use the data needed to get the total price in the desired currency. Persist the results in a new file `fixed_order_full_information.csv`, with the following columns:
* order_created_date
* order_id
* product_name
* quantity
* total_price_br
* total_price_us

Now we want to explore a little with our data. Use python to find the following information:
1. Date where we create the max amount of orders. 
2. Most demanded product and the total sell price. 
3. The top 3 most demanded categories. 

Store the results in a single CSV file named: `kpi_product_orders.csv`

## Challenge 3
Now imagine that instead of CSV files we have this information in tables in our database. 
Let’s say we have our tables: products and orders (for the purpose of this exercise, imagine that the prices are in the correct currency).
Use SQL to get the information for each of the previous points:
* The date with max amount of orders
* The most demanded product
* The top 3 most demanded categories


## Before run the code
For the correct execution of the code (specially Challenge 2), it will be necessary to fill in **pipeline.conf** file the api key value.

[Notebook with resolution](https://colab.research.google.com/drive/1Oxe_LaTJL2coK0oPjNGZPzNIRH_t9EqS?usp=sharing)
To run the Colab notebook first you need to upload the data folder and the pipeline.conf file.
