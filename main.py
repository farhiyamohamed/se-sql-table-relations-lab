# STEP 0
# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
df_boston = pd.read_sql("""SELECT e.firstName, e.lastName, e.jobTitle FROM employees e INNER JOIN offices o ON e.officeCode = o.officeCode WHERE o.city = 'Boston' ORDER BY e.firstName""", conn)

# STEP 2
df_zero_emp = pd.read_sql("""SELECT o.* FROM offices o LEFT JOIN employees e ON o.officeCode = e.officeCode GROUP BY o.officeCode HAVING COUNT(e.employeeNumber) = 0""", conn)

# STEP 3
df_employee = pd.read_sql("""SELECT e.firstName, e.lastName, o.city, o.state FROM employees e LEFT JOIN offices o ON e.officeCode = o.officeCode ORDER BY e.firstName, e.lastName""", conn)

# STEP 4
df_contacts = pd.read_sql("""SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber FROM customers c LEFT JOIN orders o ON c.customerNumber = o.customerNumber WHERE o.customerNumber IS NULL ORDER BY c.contactLastName""", conn)

# STEP 5
df_payment = pd.read_sql("""SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate FROM customers c INNER JOIN payments p ON c.customerNumber = p.customerNumber ORDER BY CAST(p.amount AS REAL) DESC""", conn)

# STEP 6
df_credit = pd.read_sql("""SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) as num_customers FROM employees e INNER JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber GROUP BY e.employeeNumber HAVING AVG(c.creditLimit) > 90000 ORDER BY num_customers DESC""", conn)

# STEP 7
df_product_sold = pd.read_sql("""SELECT p.productName, COUNT(DISTINCT od.orderNumber) as numorders, SUM(od.quantityOrdered) as totalunits FROM products p INNER JOIN orderdetails od ON p.productCode = od.productCode GROUP BY p.productCode ORDER BY totalunits DESC""", conn)

# STEP 8
df_total_customers = pd.read_sql("""SELECT p.productName, p.productCode, COUNT(DISTINCT c.customerNumber) as numpurchasers FROM products p INNER JOIN orderdetails od ON p.productCode = od.productCode INNER JOIN orders o ON od.orderNumber = o.orderNumber INNER JOIN customers c ON o.customerNumber = c.customerNumber GROUP BY p.productCode ORDER BY numpurchasers DESC""", conn)

# STEP 9
df_customers = pd.read_sql("""SELECT o.officeCode, o.city, COUNT(c.customerNumber) as n_customers FROM offices o LEFT JOIN employees e ON o.officeCode = e.officeCode LEFT JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber GROUP BY o.officeCode""", conn)

df_under_20 = pd.read_sql("""
WITH low_products AS (
  SELECT od.productCode 
  FROM orderdetails od 
  INNER JOIN orders o ON od.orderNumber = o.orderNumber 
  INNER JOIN customers c ON o.customerNumber = c.customerNumber 
  GROUP BY od.productCode 
  HAVING COUNT(DISTINCT c.customerNumber) < 20
)
SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, e.officeCode 
FROM employees e 
INNER JOIN offices o ON e.officeCode = o.officeCode 
INNER JOIN customers c2 ON e.employeeNumber = c2.salesRepEmployeeNumber 
INNER JOIN orders o2 ON c2.customerNumber = o2.customerNumber 
INNER JOIN orderdetails od2 ON o2.orderNumber = od2.orderNumber 
INNER JOIN low_products lp ON od2.productCode = lp.productCode 
ORDER BY e.firstName
""", conn)

conn.close()