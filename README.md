# Data-Modeling-101
---

#### Resources

* [Designing your First DataBase](https://www.youtube.com/watch?v=cepspxPAUTA)

* [Data Modeling Tutorial](https://www.youtube.com/watch?v=cm8UBAXwjxY&t=2s)

* [Data Model Design](https://www.youtube.com/watch?v=y2DD8tAjM0E)

* [SQLite Documentation](https://docs.python.org/3/library/sqlite3.html)

* [Mock Data Set](https://docs.google.com/spreadsheets/d/1T8-Y0zuK4mWxY6WyD7d_8661Nqc9OhS1gsZKp17iWAc/edit#gid=553297750)




## Create a SQLite Database Connection


```python
import os
import sqlite3

path = os.getcwd()

try:
    conn = sqlite3.connect(path+"/sqlite.db")
    print('Opened Database Successfully')
except Exception as e:
    print(e)
```

    Opened Database Successfully




```python
try:
    cur = conn.cursor()
    print("Cursor Object Created")
except Exception as e:
    print(e)    
```

    Cursor Object Created


## Create Tables


```python
try:
    cur.executescript('''
            CREATE TABLE customers 
            (
              customer_id VARCHAR(8) PRIMARY KEY NOT NULL 
            , first_name VARCHAR(50) NOT NULL 
            , last_name VARCHAR(50) NOT NULL
            , email VARCHAR(50)
            , address VARCHAR(50) NOT NULL
            , city VARCHAR(50) NOT NULL
            , state VARCHAR(50)
            , country VARCHAR(50)
            );
            
            CREATE TABLE orders 
            (
              order_id VARCHAR(6) PRIMARY KEY NOT NULL 
            , customer_id VARCHAR(8) NOT NULL 
            , order_date DATE NOT NULL 
            , FOREIGN KEY (customer_id)
                           REFERENCES customers (customer_id)
            );
            
            CREATE TABLE order_details
            (
              order_id VARCHAR(6) NOT NULL 
            , product_id VARCHAR(10) NOT NULL
            , quantity INT NOT NULL
            , PRIMARY KEY (order_id, product_id)                
            );
            
            CREATE TABLE product 
            (
              product_id VARCHAR(10) PRIMARY KEY NOT NULL 
            , name VARCHAR(50) NOT NULL
            , cost MONEY NOT NULL
            , price MONEY NOT NULL
            )
            
            ''')
    
    print('Tables created successfully')

except Exception as e:
    print(e)  
```

    Tables created successfully


## Insert Data into SQLite DB


```python
import pandas as pd
import glob

files_path = path+"/data_files/"

# obtain all files in 'data_files' folder
csv_files = glob.glob(os.path.join(files_path, "*.csv"))

# loop over the list of csv files
df_list = []

for i in csv_files:
    # read the csv file
    df = pd.read_csv(i)
    table_name = str(i[65:-4])
    df.to_sql(table_name,con=conn,if_exists='append',index=False)
    print(f'Data added to {table_name}')
```

    Data added to customers
    Data added to orders
    Data added to order_details
    Data added to product


## Confirm Correct Insertion of Data


```python
display(pd.read_sql('''select * from customers limit 5''',conn))
display(pd.read_sql('''select * from orders limit 5''',conn))
display(pd.read_sql('''select * from order_details limit 5''',conn))
display(pd.read_sql('''select * from product limit 5''',conn))
```


<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>email</th>
      <th>address</th>
      <th>city</th>
      <th>state</th>
      <th>country</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>51829185</td>
      <td>Adiana</td>
      <td>Greaves</td>
      <td>agreaves0@wordpress.com</td>
      <td>185 Summit Junction</td>
      <td>Nizhyn</td>
      <td>None</td>
      <td>Ukraine</td>
    </tr>
    <tr>
      <th>1</th>
      <td>37652342</td>
      <td>Dolli</td>
      <td>Pestor</td>
      <td>None</td>
      <td>5822 Comanche Lane</td>
      <td>Vyksa</td>
      <td>None</td>
      <td>Russia</td>
    </tr>
    <tr>
      <th>2</th>
      <td>91376671</td>
      <td>Veronica</td>
      <td>Gedling</td>
      <td>vgedling2@jugem.jp</td>
      <td>28752 Pankratz Lane</td>
      <td>Sukaraharja</td>
      <td>None</td>
      <td>Indonesia</td>
    </tr>
    <tr>
      <th>3</th>
      <td>23518886</td>
      <td>Brynne</td>
      <td>Gorgler</td>
      <td>bgorgler3@wikispaces.com</td>
      <td>914 Myrtle Lane</td>
      <td>Baipu</td>
      <td>None</td>
      <td>China</td>
    </tr>
    <tr>
      <th>4</th>
      <td>12848729</td>
      <td>Barnabas</td>
      <td>Wasielewicz</td>
      <td>bwasielewicz4@miibeian.gov.cn</td>
      <td>3 Clyde Gallagher Center</td>
      <td>Alunda</td>
      <td>Uppsala</td>
      <td>Sweden</td>
    </tr>
  </tbody>
</table>
</div>



<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>customer_id</th>
      <th>order_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>438948</td>
      <td>51829185</td>
      <td>7/20/2021</td>
    </tr>
    <tr>
      <th>1</th>
      <td>741649</td>
      <td>37652342</td>
      <td>10/23/2017</td>
    </tr>
    <tr>
      <th>2</th>
      <td>152787</td>
      <td>91376671</td>
      <td>11/23/2021</td>
    </tr>
    <tr>
      <th>3</th>
      <td>627744</td>
      <td>23518886</td>
      <td>8/16/2020</td>
    </tr>
    <tr>
      <th>4</th>
      <td>116772</td>
      <td>12848729</td>
      <td>1/5/2021</td>
    </tr>
  </tbody>
</table>
</div>



<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>product_id</th>
      <th>quantity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>438948</td>
      <td>8249541974</td>
      <td>66</td>
    </tr>
    <tr>
      <th>1</th>
      <td>741649</td>
      <td>1133448411</td>
      <td>20</td>
    </tr>
    <tr>
      <th>2</th>
      <td>152787</td>
      <td>3247643876</td>
      <td>97</td>
    </tr>
    <tr>
      <th>3</th>
      <td>627744</td>
      <td>5982551725</td>
      <td>75</td>
    </tr>
    <tr>
      <th>4</th>
      <td>116772</td>
      <td>3387882238</td>
      <td>73</td>
    </tr>
  </tbody>
</table>
</div>



<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_id</th>
      <th>name</th>
      <th>cost</th>
      <th>price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>8249541974</td>
      <td>Lamb - Rack</td>
      <td>$94.99</td>
      <td>$1,140.02</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1133448411</td>
      <td>Cheese - Le Cru Du Clocher</td>
      <td>$84.32</td>
      <td>$1,591.35</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3247643876</td>
      <td>Vaccum Bag 10x13</td>
      <td>$31.83</td>
      <td>$1,992.35</td>
    </tr>
    <tr>
      <th>3</th>
      <td>5982551725</td>
      <td>Bag Stand</td>
      <td>$35.70</td>
      <td>$564.99</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3387882238</td>
      <td>Tea Leaves - Oolong</td>
      <td>$3.25</td>
      <td>$1,864.88</td>
    </tr>
  </tbody>
</table>
</div>

