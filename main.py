import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('data.sqlite')

print(pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn))

# =========================
# PART 1: JOIN AND FILTER
# =========================

# 1. Employees in Boston
boston_employees = pd.read_sql("""
SELECT 
    e.firstName,
    e.lastName,
    e.jobTitle
FROM employees e
JOIN offices o
    ON e.officeCode = o.officeCode
WHERE o.city = 'Boston'
""", conn)

print("\nBOSTON EMPLOYEES:\n", boston_employees)

# 2. Offices with zero employees
empty_offices = pd.read_sql("""
SELECT 
    o.officeCode,
    o.city,
    o.state
FROM offices o
LEFT JOIN employees e
    ON o.officeCode = e.officeCode
WHERE e.employeeNumber IS NULL
""", conn)

print("\nEMPTY OFFICES:\n", empty_offices)


# =========================
# PART 2: TYPE OF JOIN
# =========================

# 3. All employees + office info
all_employees = pd.read_sql("""
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

print("\nALL EMPLOYEES:\n", all_employees)

# 4. Customers with NO orders
no_orders_customers = pd.read_sql("""
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

print("\nCUSTOMERS WITH NO ORDERS:\n", no_orders_customers)


# =========================
# PART 3: PAYMENTS
# =========================

payments_report = pd.read_sql("""
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

print("\nPAYMENTS:\n", payments_report)


# =========================
# PART 4: GROUPING
# =========================

# 5. Employees with high credit customers
high_credit_employees = pd.read_sql("""
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

print("\nHIGH CREDIT EMPLOYEES:\n", high_credit_employees)


# 6. Top-selling products
top_products = pd.read_sql("""
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

print("\nTOP PRODUCTS:\n", top_products)


# =========================
# PART 5: MULTIPLE JOINS
# =========================

# 7. Product customers count
product_customers = pd.read_sql("""
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

print("\nPRODUCT CUSTOMERS:\n", product_customers)


# 8. Customers per office
customers_per_office = pd.read_sql("""
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

print("\nCUSTOMERS PER OFFICE:\n", customers_per_office)


# =========================
# PART 6: SUBQUERY
# =========================

low_product_employees = pd.read_sql("""
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

print("\nLOW PRODUCT EMPLOYEES:\n", low_product_employees)


# Close connection
conn.close()