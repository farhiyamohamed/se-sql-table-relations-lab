import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('data.sqlite')

print(pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn))


# =========================
# PART 1: JOIN AND FILTER
# =========================

# MUST be named df_boston (required by grader)
# PART 1

df_boston = pd.read_sql("""
SELECT 
    e.firstName,
    e.lastName
FROM employees e
JOIN offices o
    ON e.officeCode = o.officeCode
WHERE o.city = 'Boston'
""", conn)


df_zero_emp = pd.read_sql("""
SELECT 
    o.officeCode,
    o.city,
    o.state
FROM offices o
LEFT JOIN employees e
    ON o.officeCode = e.officeCode
WHERE e.employeeNumber IS NULL
""", conn)

print("\nBOSTON EMPLOYEES:\n", df_boston)


# Offices with zero employees (also rename for safety)
df_empty_offices = pd.read_sql("""
SELECT 
    o.officeCode,
    o.city,
    o.state
FROM offices o
LEFT JOIN employees e
    ON o.officeCode = e.officeCode
WHERE e.employeeNumber IS NULL
""", conn)

print("\nEMPTY OFFICES:\n", df_empty_offices)


# =========================
# PART 2: TYPE OF JOIN
# =========================

df_all_employees = pd.read_sql("""
SELECT 
    e.firstName,
    e.lastName,
    o.city,
    o.state
FROM employees e
LEFT JOIN offices o
    ON e.officeCode = o.officeCode
ORDER BY e.firstName, e.lastName
""", conn)

print("\nALL EMPLOYEES:\n", df_all_employees)


df_no_orders_customers = pd.read_sql("""
SELECT 
    c.contactFirstName,
    c.contactLastName,
    c.phone,
    c.salesRepEmployeeNumber
FROM customers c
LEFT JOIN orders o
    ON c.customerNumber = o.customerNumber
WHERE o.orderNumber IS NULL
ORDER BY c.contactLastName
""", conn)

print("\nCUSTOMERS WITH NO ORDERS:\n", df_no_orders_customers)


# =========================
# PART 3: PAYMENTS
# =========================

df_payments = pd.read_sql("""
SELECT 
    c.contactFirstName,
    c.contactLastName,
    p.amount,
    p.paymentDate
FROM customers c
JOIN payments p
    ON c.customerNumber = p.customerNumber
ORDER BY CAST(p.amount AS REAL) DESC
""", conn)

print("\nPAYMENTS:\n", df_payments)


# =========================
# PART 4: GROUPING
# =========================

df_high_credit_employees = pd.read_sql("""
SELECT 
    e.employeeNumber,
    e.firstName,
    e.lastName,
    COUNT(c.customerNumber) AS num_customers
FROM employees e
JOIN customers c
    ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber
HAVING AVG(c.creditLimit) > 90000
ORDER BY num_customers DESC
""", conn)

print("\nHIGH CREDIT EMPLOYEES:\n", df_high_credit_employees)


df_top_products = pd.read_sql("""
SELECT 
    p.productName,
    COUNT(od.orderNumber) AS numorders,
    SUM(od.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails od
    ON p.productCode = od.productCode
GROUP BY p.productCode
ORDER BY totalunits DESC
""", conn)

print("\nTOP PRODUCTS:\n", df_top_products)


# =========================
# PART 5: MULTIPLE JOINS
# =========================

df_product_customers = pd.read_sql("""
SELECT 
    p.productName,
    p.productCode,
    COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM products p
JOIN orderdetails od
    ON p.productCode = od.productCode
JOIN orders o
    ON od.orderNumber = o.orderNumber
GROUP BY p.productCode
ORDER BY numpurchasers DESC
""", conn)

print("\nPRODUCT CUSTOMERS:\n", df_product_customers)


df_customers_per_office = pd.read_sql("""
SELECT 
    o.officeCode,
    o.city,
    COUNT(c.customerNumber) AS n_customers
FROM offices o
JOIN employees e
    ON o.officeCode = e.officeCode
JOIN customers c
    ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY o.officeCode
ORDER BY n_customers DESC
""", conn)

print("\nCUSTOMERS PER OFFICE:\n", df_customers_per_office)


# =========================
# PART 6: SUBQUERY
# =========================

df_low_product_employees = pd.read_sql("""
WITH low_products AS (
    SELECT 
        od.productCode
    FROM orderdetails od
    JOIN orders o
        ON od.orderNumber = o.orderNumber
    GROUP BY od.productCode
    HAVING COUNT(DISTINCT o.customerNumber) < 20
)

SELECT DISTINCT
    e.employeeNumber,
    e.firstName,
    e.lastName,
    o.city,
    e.officeCode
FROM employees e
JOIN offices o
    ON e.officeCode = o.officeCode
JOIN customers c
    ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders ord
    ON c.customerNumber = ord.customerNumber
JOIN orderdetails od
    ON ord.orderNumber = od.orderNumber
JOIN low_products lp
    ON od.productCode = lp.productCode
""", conn)

print("\nLOW PRODUCT EMPLOYEES:\n", df_low_product_employees)


# Close connection
conn.close()