# Data Modeling Project

Create a Database using SQLITE and populate the database using data pulled from different sources.

---

#### Resources

* [Designing your First DataBase](https://www.youtube.com/watch?v=cepspxPAUTA)

* [Data Modeling Tutorial](https://www.youtube.com/watch?v=cm8UBAXwjxY&t=2s)

* [Data Model Design](https://www.youtube.com/watch?v=y2DD8tAjM0E)

* [SQLite Documentation](https://docs.python.org/3/library/sqlite3.html)

* [Mock Data Set](https://docs.google.com/spreadsheets/d/1T8-Y0zuK4mWxY6WyD7d_8661Nqc9OhS1gsZKp17iWAc/edit#gid=553297750)


![Screen Shot 2022-01-11 at 4.33.03 PM.png](attachment:b340ad2b-2918-4d42-86d7-826566e11e10.png)

# Create a SQLite Database Connection


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


# Create Tables


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
            , cost INT NOT NULL
            , price INT NOT NULL
            )
            
            ''')
    
    print('Tables created successfully')

except Exception as e:
    print(e)  
```

    Tables created successfully


# Insert Data into SQLite DB


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


# Confirm Correct Insertion of Data


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
      <td>94.99</td>
      <td>1140.02</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1133448411</td>
      <td>Cheese - Le Cru Du Clocher</td>
      <td>84.32</td>
      <td>1591.35</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3247643876</td>
      <td>Vaccum Bag 10x13</td>
      <td>31.83</td>
      <td>1992.35</td>
    </tr>
    <tr>
      <th>3</th>
      <td>5982551725</td>
      <td>Bag Stand</td>
      <td>35.70</td>
      <td>564.99</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3387882238</td>
      <td>Tea Leaves - Oolong</td>
      <td>3.25</td>
      <td>1864.88</td>
    </tr>
  </tbody>
</table>
</div>


# Now let's write some SQL!

What are the top paying countries? 


```python
df = pd.read_sql('''

select 
c.country,
sum(p.price) as price

from customers c
left join orders o on c.customer_id = o.customer_id
left join order_details od on od.order_id = o.order_id
left join product p on p.product_id = od.product_id

group by 1
having sum(p.price) > 20000
order by 2 desc

''',conn)

df
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>country</th>
      <th>price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>China</td>
      <td>429985.97</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Indonesia</td>
      <td>232216.14</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Russia</td>
      <td>114390.28</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Philippines</td>
      <td>97332.99</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Poland</td>
      <td>82341.95</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Brazil</td>
      <td>77541.27</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Portugal</td>
      <td>74390.58</td>
    </tr>
    <tr>
      <th>7</th>
      <td>United States</td>
      <td>55933.80</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Sweden</td>
      <td>54274.93</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Canada</td>
      <td>49239.78</td>
    </tr>
    <tr>
      <th>10</th>
      <td>France</td>
      <td>47019.26</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Czech Republic</td>
      <td>45726.32</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Japan</td>
      <td>42888.76</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Ukraine</td>
      <td>35869.74</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Greece</td>
      <td>34906.73</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Peru</td>
      <td>31964.18</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Mexico</td>
      <td>30851.54</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Nigeria</td>
      <td>25969.75</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Colombia</td>
      <td>24973.82</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Iran</td>
      <td>24547.68</td>
    </tr>
    <tr>
      <th>20</th>
      <td>Thailand</td>
      <td>21015.45</td>
    </tr>
  </tbody>
</table>
</div>



Who are the top paying customers?


```python
df2 = pd.read_sql('''

select 
c.customer_id,
c.last_name,
sum(p.price) as price


from customers c
left join orders o on c.customer_id = o.customer_id
left join order_details od on od.order_id = o.order_id
left join product p on p.product_id = od.product_id

where price not null and quantity not null

group by 1,2
having max(od.quantity) > 100
order by 3 desc

limit 10


''',conn)

df2
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>last_name</th>
      <th>price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>83411444</td>
      <td>Raiman</td>
      <td>9220.37</td>
    </tr>
    <tr>
      <th>1</th>
      <td>28564389</td>
      <td>Southway</td>
      <td>7450.25</td>
    </tr>
    <tr>
      <th>2</th>
      <td>96551985</td>
      <td>Manthorpe</td>
      <td>6978.37</td>
    </tr>
    <tr>
      <th>3</th>
      <td>52479296</td>
      <td>Whitnall</td>
      <td>6522.63</td>
    </tr>
    <tr>
      <th>4</th>
      <td>99223253</td>
      <td>Hawtry</td>
      <td>6489.35</td>
    </tr>
    <tr>
      <th>5</th>
      <td>82168535</td>
      <td>Fraczkiewicz</td>
      <td>6039.03</td>
    </tr>
    <tr>
      <th>6</th>
      <td>15362265</td>
      <td>Gervaise</td>
      <td>6032.89</td>
    </tr>
    <tr>
      <th>7</th>
      <td>26178698</td>
      <td>Silverton</td>
      <td>5464.27</td>
    </tr>
    <tr>
      <th>8</th>
      <td>85678492</td>
      <td>Lawful</td>
      <td>5380.63</td>
    </tr>
    <tr>
      <th>9</th>
      <td>56278241</td>
      <td>Parlet</td>
      <td>5370.79</td>
    </tr>
  </tbody>
</table>
</div>



What product is purchased the most and the revenue associated for the following product IDs:

* 8249541974
* 1133448411
* 3247643876


```python
df3 = pd.read_sql('''

select 
c.customer_id,
p.name,
sum(od.quantity) as quantity,
sum(p.price) as price


from customers c
left join orders o on c.customer_id = o.customer_id
left join order_details od on od.order_id = o.order_id
left join product p on p.product_id = od.product_id

where 
p.price not null and 
od.quantity not null and 
p.product_id in ('8249541974','1133448411','3247643876')


group by 1,2
order by 3 desc

limit 15


''',conn)

df3
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>name</th>
      <th>quantity</th>
      <th>price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>84793743</td>
      <td>Cheese - Le Cru Du Clocher</td>
      <td>122</td>
      <td>3182.70</td>
    </tr>
    <tr>
      <th>1</th>
      <td>47316639</td>
      <td>Vaccum Bag 10x13</td>
      <td>104</td>
      <td>3984.70</td>
    </tr>
    <tr>
      <th>2</th>
      <td>98772997</td>
      <td>Vaccum Bag 10x13</td>
      <td>100</td>
      <td>1992.35</td>
    </tr>
    <tr>
      <th>3</th>
      <td>91376671</td>
      <td>Vaccum Bag 10x13</td>
      <td>97</td>
      <td>1992.35</td>
    </tr>
    <tr>
      <th>4</th>
      <td>81123221</td>
      <td>Vaccum Bag 10x13</td>
      <td>90</td>
      <td>1992.35</td>
    </tr>
    <tr>
      <th>5</th>
      <td>33155552</td>
      <td>Vaccum Bag 10x13</td>
      <td>81</td>
      <td>1992.35</td>
    </tr>
    <tr>
      <th>6</th>
      <td>35636653</td>
      <td>Lamb - Rack</td>
      <td>80</td>
      <td>1140.02</td>
    </tr>
    <tr>
      <th>7</th>
      <td>12315757</td>
      <td>Vaccum Bag 10x13</td>
      <td>75</td>
      <td>1992.35</td>
    </tr>
    <tr>
      <th>8</th>
      <td>36429957</td>
      <td>Cheese - Le Cru Du Clocher</td>
      <td>74</td>
      <td>1591.35</td>
    </tr>
    <tr>
      <th>9</th>
      <td>51829185</td>
      <td>Lamb - Rack</td>
      <td>66</td>
      <td>1140.02</td>
    </tr>
    <tr>
      <th>10</th>
      <td>77298596</td>
      <td>Lamb - Rack</td>
      <td>66</td>
      <td>1140.02</td>
    </tr>
    <tr>
      <th>11</th>
      <td>24693947</td>
      <td>Lamb - Rack</td>
      <td>63</td>
      <td>1140.02</td>
    </tr>
    <tr>
      <th>12</th>
      <td>65963634</td>
      <td>Vaccum Bag 10x13</td>
      <td>63</td>
      <td>1992.35</td>
    </tr>
    <tr>
      <th>13</th>
      <td>14679638</td>
      <td>Cheese - Le Cru Du Clocher</td>
      <td>61</td>
      <td>1591.35</td>
    </tr>
    <tr>
      <th>14</th>
      <td>57642398</td>
      <td>Cheese - Le Cru Du Clocher</td>
      <td>59</td>
      <td>1591.35</td>
    </tr>
  </tbody>
</table>
</div>



Rank products purchased the most by customers and find the total quantity of products?


```python
df4 = pd.read_sql('''

select 

c.last_name,
od.product_id,
od.quantity,
sum(od.quantity) OVER (partition by od.product_id order by od.quantity) as total_quantity

from order_details od
left join orders o on od.order_id = o.order_id 
left join customers c on o.customer_id = c.customer_id

order by 4 desc

limit 20



''',conn)

df4
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>last_name</th>
      <th>product_id</th>
      <th>quantity</th>
      <th>total_quantity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Houltham</td>
      <td>2569785328</td>
      <td>102</td>
      <td>8128</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Petheridge</td>
      <td>2569785328</td>
      <td>102</td>
      <td>8128</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Welch</td>
      <td>2569785328</td>
      <td>101</td>
      <td>7924</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Rewan</td>
      <td>2569785328</td>
      <td>101</td>
      <td>7924</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Dytham</td>
      <td>2569785328</td>
      <td>100</td>
      <td>7722</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Warr</td>
      <td>2569785328</td>
      <td>100</td>
      <td>7722</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Bazelle</td>
      <td>2569785328</td>
      <td>99</td>
      <td>7522</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Haack</td>
      <td>2569785328</td>
      <td>98</td>
      <td>7423</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Wheelband</td>
      <td>2569785328</td>
      <td>95</td>
      <td>7325</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Lathee</td>
      <td>2569785328</td>
      <td>94</td>
      <td>7230</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Broschke</td>
      <td>2569785328</td>
      <td>94</td>
      <td>7230</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Gayton</td>
      <td>2569785328</td>
      <td>94</td>
      <td>7230</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Piscopiello</td>
      <td>2569785328</td>
      <td>94</td>
      <td>7230</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Breston</td>
      <td>2569785328</td>
      <td>92</td>
      <td>6854</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Edwardson</td>
      <td>2569785328</td>
      <td>92</td>
      <td>6854</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Badland</td>
      <td>2569785328</td>
      <td>90</td>
      <td>6670</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Hawtry</td>
      <td>2569785328</td>
      <td>90</td>
      <td>6670</td>
    </tr>
    <tr>
      <th>17</th>
      <td>De Gouy</td>
      <td>2569785328</td>
      <td>89</td>
      <td>6490</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Hiddsley</td>
      <td>2569785328</td>
      <td>89</td>
      <td>6490</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Laise</td>
      <td>2569785328</td>
      <td>89</td>
      <td>6490</td>
    </tr>
  </tbody>
</table>
</div>
