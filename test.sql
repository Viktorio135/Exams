CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    amount DECIMAL(10, 2),
    order_date DATE
);


-- SELECT (name) from customers
-- JOIN orders on orders.customer_id = customers.id
-- GROUP BY orders.customer_id
-- HAVING SUM(orders.amount);


SELECT customers.name SUM(orders.amount) as total_amount
FROM customers
JOIN orders on orders.customer_id = customers.id
GROUP BY customers.id, customers.name
HAVING SUM(orders.amount) > 1000;


SELECT customers.name from customers
LEFT JOIN orders on orders.customer_id = customers.id
WHERE orders.id IS NULL;

Вопросы:
Напиши запрос, который вернёт имена клиентов и сумму всех их заказов.

Выведи только тех клиентов, у которых сумма заказов больше 1000.

Найди клиентов, у которых нет ни одного заказа.

Напиши запрос, который покажет кол-во заказов по месяцам.