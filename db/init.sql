/* Creacion de la tabla companies. Se le asigna
un ID como llave primaria */
CREATE TABLE IF NOT EXISTS companies (
id VARCHAR(24) PRIMARY KEY,
name VARCHAR(130) NOT NULL
);

/* Creacion de la tabla transacciones. Se respetan
los tipos de datos exactos amount decimal(6,12) y
created_at timestamp */
CREATE TABLE IF NOT EXISTS charges (
id VARCHAR(24) PRIMARY KEY,
company_id VARCHAR(24) NOT NULL,
amount DECIMAL(16,2) NOT NULL,
status VARCHAR(30) NOT NULL,
created_at TIMESTAMP NOT NULL,
updated_at TIMESTAMP NULL,
CONSTRAINT fk_company
FOREIGN KEY(company_id)
REFERENCES companies(id)
);

/* Creacion de la vista
Usa un JOIN para unir ambas tablas y un GROUP BY para calcular
el monto total transaccionado por día para las diferentes compañías*/
CREATE OR REPLACE VIEW vw_daily_transactions AS
SELECT
c.name AS company_name,
DATE(ch.created_at) AS transaction_date,
SUM(ch.amount) AS total_amount
FROM
charges ch
JOIN
companies c ON ch.company_id = c.id
GROUP BY
c.name,
DATE(ch.created_at)
ORDER BY
transaction_date DESC,
company_name ASC;
