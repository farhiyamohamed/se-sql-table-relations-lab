import sqlite3
import pandas as pd

conn = sqlite3.connect('data.sqlite')

# =========================
# PART 1: JOIN AND FILTER
# =========================

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


# =========================
# =========================
# PART 2: TYPE OF JOIN
# =========================

df_contacts = pd.read_sql("""
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


df_no_orders = pd.read_sql("""
SELECT 
    c.contactFirstName,
    c.contactLastName,
    c.phone,
    c.salesRepEmployeeNumber
FROM customers c
LEFT JOIN orders o
    ON c.customerNumber = o.customerNumber
WHERE o.orderNumber IS NULL
""", conn)


# =========================
# PART 3: BUILT-IN FUNCTION
# =========================

df_payments = pd.read_sql("""
SELECT 
    c.contactFirstName,
    c.contactLastName,
    CAST(p.amount AS REAL) AS amount,
    p.paymentDate
FROM customers c
JOIN payments p
    ON c.customerNumber = p.customerNumber
ORDER BY CAST(p.amount AS REAL) DESC
""", conn)


# =========================
# PART 4: JOINING + GROUPING
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
GROUP BY e.employeeNumber, e.firstName, e.lastName
HAVING AVG(c.creditLimit) > 90000
ORDER BY num_customers DESC
""", conn)


df_top_products = pd.read_sql("""
SELECT 
    p.productName,
    COUNT(od.orderNumber) AS numorders,
    SUM(od.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails od
    ON p.productCode = od.productCode
GROUP BY p.productCode, p.productName
ORDER BY totalunits DESC
""", conn)


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
GROUP BY p.productCode, p.productName
ORDER BY numpurchasers DESC
""", conn)


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
GROUP BY o.officeCode, o.city
ORDER BY n_customers DESC
""", conn)


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


conn.close()