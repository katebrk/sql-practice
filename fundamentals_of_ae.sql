-- Code Modularity
-- Problem: Analyze the average consumer discount on products over time. 

-- Initial query
SELECT
    D.product_id,
    D.date,
    P.product_name,
    -- Apply VAT to retail price and substract selling price including VAT to calculate discount amount
    (CASE
        WHEN P.product_category = 1 THEN P.retail_price_excl_vat * 1.09
        WHEN P.product_category = 2 THEN P.retail_price_excl_vat * 1.21
        WHEN P.product_category = 3 THEN P.retail_price_excl_vat
        ELSE P.retail_price_excl_vat * 1.21
    END)- D.avg_selling_price as avg_discount_amount_incl_vat
FROM (
    SELECT date,
        product_id,
        ROUND(AVG(amount/quantity), 2) AS avg_selling_price
    FROM bakery.Sales
    GROUP BY
        date,
        product_id
) AS D
LEFT JOIN bakery.Products as P ON D.product_id = P.id
GROUP BY
    D.product_id,
    D.date

-- We need to refactor the code above to make it modular, more readable and DRY

-- OPTION 1.
-- Step 1 - create UDF to apply VAT to retail prices 
-- This UDF or macro is defined outside your query to be reused elsewhere and managed centrally. 
CREATE FUNCTION apply_vat(price FLOAT, product_category INT)
RETURNS FLOAT64
AS (
    CASE
        WHEN product_category = 1 THEN price * 1.09
        WHEN product_category = 2 THEN price * 1.21
        WHEN product_category = 3 THEN price
        ELSE price * 1.21
    END
);

-- Step 2 - withing SQL file calculate the daily selling prices of the products
WITH daily_selling_prices AS (
    SELECT
        S.date,
        S.product_id,
        ROUND(AVG(S.amount/S.quantity), 2) AS avg_selling_price
    FROM
        bakery.Sales AS S
    GROUP BY
        S.date,
        S.product_id
)

-- Step 3 - calculate the discount applied every day to products based on the difference between the selling price and retail price, including VAT.
SELECT
    S.product_id,
    S.date,
    P.product_name,
    apply_vat(P.retail_price_excl_vat, P.product_category) - S.avg_
selling_price as avg_discount_amount_incl_vat
FROM
    daily_selling_prices as S
LEFT JOIN
    bakery.Products as P ON S.product_id = P.id
GROUP BY
S.product_id,
    S.date,
    P.product_name


-- OPTION 2. The same, but in dbt 
-- Step 1 - create macros to apply VAT to retail prices 
{% macro apply_vat(price_column, product_category_column) %}
    CASE
WHEN {{  product_category_column }} = 1 THEN {{  price_column }} * 1.09
WHEN {{  product_category_column }} = 2 THEN {{  price_column }} * 1.21
WHEN {{  product_category_column }} = 3 THEN {{  price_column }}
ELSE {{  price_column }} * 1.21
END
{% endmacro %}

-- Step 2 - withing SQL file calculate the daily selling prices of the products
WITH daily_selling_prices AS (
    SELECT
        S.date,
        S.product_id,
        ROUND(AVG(S.amount/S.quantity), 2) AS avg_selling_price
    FROM
        {{ref('Sales') }}
    GROUP BY
        S.date,
        S.product_id
)

-- Step 3 - calculate the discount applied every day to products based on the difference between the selling price and retail price, including VAT.
SELECT
    S.product_id,
    S.date,
    P.product_name,
    {{ apply_vat('retail_price_excl_vat', 'product_category') }} -
S.avg_selling_price as avg_discount_amount_incl_vat
FROM {{ ref('daily_selling_prices') }} as S
LEFT JOIN {{ ref('Products') }} as P
ON S.product_id = P.id
GROUP BY
    S.product_id,
    S.date,
    P.product_name
