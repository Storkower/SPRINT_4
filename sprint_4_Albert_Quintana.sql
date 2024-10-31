/* SPRINT_4
Tasca S4.01. Creació de Base de Dades
Descripció
Partint d'alguns arxius CSV dissenyaràs i crearàs la teva base de dades.
Nivell 1
Descàrrega els arxius CSV, estudia'ls i dissenya una base de dades amb un esquema d'estrella que contingui, almenys 4 taules de les quals puguis realitzar les següents consultes:
*/
 
-- Creem la BDD
CREATE DATABASE IF NOT EXISTS sprint_4;
-- aseguro la connexio
SET NAMES 'utf8mb4';
SET CHARACTER SET 'utf8mb4';
SET sql_mode = '';

-- Fem servi la BDD
USE Sprint_4;

-- Creem les taules

-- Creem les taula de dimensions
-- Taula dimensió usuaris. Aquí afegirem els 3 csv d'users.
CREATE TABLE IF NOT EXISTS Dim_users (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    surname VARCHAR(50),
    phone VARCHAR(15),
    email VARCHAR(100),
    birth_date VARCHAR(13),
    country VARCHAR(50),
    city VARCHAR(50),
    postal_code VARCHAR(10),
    address VARCHAR(255)
);

-- Importar Dim_Usuario 
-- Primer importem "users_usa.csv"

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users_usa.csv"
INTO TABLE Dim_users
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(id, name, surname, phone, email, birth_date, country, city, postal_code, address)
SET birth_date = STR_TO_DATE(birth_date, "%b %d, %Y");

-- Repetir-ho per "users_uk.csv"
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users_uk.csv"
INTO TABLE Dim_users
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(id, name, surname, phone, email, birth_date, country, city, postal_code, address)
SET birth_date = STR_TO_DATE(birth_date, "%b %d, %Y");

-- Repetir-ho per "users_ca.csv"
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users_ca.csv"
INTO TABLE Dim_users
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(id, name, surname, phone, email, birth_date, country, city, postal_code, address)
SET birth_date = STR_TO_DATE(birth_date, "%b %d, %Y");

-- Modifiquem la columna de birth_date de tipus VARCHAR a DATE
ALTER TABLE dim_users
MODIFY birth_date DATE;

-- Taula dimensió empreses
CREATE TABLE IF NOT EXISTS Dim_companies (
    company_id VARCHAR(50) PRIMARY KEY,
    company_name VARCHAR(100),
    phone VARCHAR(15),
    email VARCHAR(100),
    country VARCHAR(50),
    website VARCHAR(100)
);

-- Dim_companies
-- Carreguem "companies.csv"
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/companies.csv"
INTO TABLE Dim_companies
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY  '\r\n'
IGNORE 1 ROWS
(company_id, company_name, phone, email, country, website);

-- Taula dimensió targetes de crédit
CREATE TABLE IF NOT EXISTS Dim_credit_cards (
    id VARCHAR(20) PRIMARY KEY,
    user_id INT,
    iban VARCHAR(50),
    pan VARCHAR(20),
	pin VARCHAR(4), -- Per si comença per 0 que no l'elimini
    cvv VARCHAR(3), -- Per si comença per 0 que no l'elimini
    track1 VARCHAR(150),
    track2 VARCHAR(150),
    expiring_date VARCHAR(15)
);

-- Dim_credit_cards
-- Carreguem "credit_cards.csv"
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/credit_cards.csv"
INTO TABLE Dim_credit_cards
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY  '\r\n'
IGNORE 1 ROWS
(id, user_id, iban, @pan_temp, pin, cvv, track1, track2, @expiring_date_temp)
SET	
	pan = REPLACE(@pan_temp, ' ', ''),
	expiring_date = STR_TO_DATE (@expiring_date_temp, '%m/%d/%y');

-- Al no carregar les dades miro els warnings. NO hi ha res.
SHOW WARNINGS;

-- Miro el character set de la BDD, correcte.
SHOW VARIABLES LIKE 'character_set_database';

-- Miro el character set de la taula, correcte.
SHOW CREATE TABLE Dim_credit_cards;

/*
Com que no funciona amb Workbench ni amb Command Line Client o faig amb Wizzard. 
Però hem de treballar el format data de la columna expiring_date i eliminar espais de la columna pan
*/
-- Actualitzar les dades existents
UPDATE Dim_credit_cards
SET expiring_date = STR_TO_DATE(expiring_date, '%m/%d/%y');

-- Modificar la columna per a que sigui de tipus DATE
ALTER TABLE Dim_credit_cards
MODIFY expiring_date DATE;

-- Eliminar espai columna pan
UPDATE Dim_credit_cards
SET pan = REPLACE(pan, ' ', '');

-- Taula dimensió productes
CREATE TABLE IF NOT EXISTS Dim_products (
    id INT PRIMARY KEY,
    product_name VARCHAR(100),
    price VARCHAR(10),
    colour VARCHAR(50),
    weight DECIMAL(10, 2),
    warehouse_id VARCHAR(50)
);

-- Dim_products
-- Carreguem "products.csv"
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/products.csv"
INTO TABLE Dim_products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY  '\r\n'
IGNORE 1 ROWS
(id, product_name, @price_temp, colour, weight, warehouse_id)
SET price = REPLACE(@price_temp, '$', '');

/*
Aquí em passa el mateix que amb la taula dim_credit_cards. No puc carregar les dades amb codi i ho faig amb Wizzard.
Com que no he pogut fer el replace del $ en la pujada ho faig a continuació.
*/

-- Eliminar espai columna pan
UPDATE Dim_products
SET price = REPLACE(price, '$', '');

-- Modifiquem el tipus de dada de la columna
ALTER TABLE Dim_products
MODIFY price DECIMAL(10,2);

-- Taula dimensió fecha
CREATE TABLE IF NOT EXISTS Dim_fecha (
    fecha DATE PRIMARY KEY,
    dia INT,
    dia_setmana VARCHAR(10),
    mes INT,
    mes_nom VARCHAR(15),
    trimestre INT,
    semestre INT,
    año INT
);

-- Creem un procediment per carregar les dades a la taula 

-- Em dona talls de connexió i ho incremento a 8 hores
SET GLOBAL wait_timeout = 28800;
SET GLOBAL interactive_timeout = 28800;

-- aquí comença el procediment i pel tema dels talls ho faig entre 2020 i 2030 amb un límit de 999 dies de carrega per interval.

DELIMITER //

CREATE PROCEDURE LoadDimFecha()
BEGIN
    DECLARE current_fecha DATE; 
    DECLARE limit_date DATE;
    SET current_fecha = '2020-01-01';

    WHILE current_fecha <= '2030-12-31' DO
        SET limit_date = DATE_ADD(current_fecha, INTERVAL 999 DAY); 

        WHILE current_fecha <= limit_date AND current_fecha <= '2030-12-31' DO
            INSERT INTO Dim_fecha (fecha, dia, dia_setmana, mes, mes_nom, trimestre, semestre, año)
            VALUES (
                current_fecha,
                DAY(current_fecha),
                DAYNAME(current_fecha),
                MONTH(current_fecha),
                MONTHNAME(current_fecha),
                QUARTER(current_fecha),
                CASE 
                    WHEN QUARTER(current_fecha) IN (1, 2) THEN 1
                    ELSE 2 
                END,
                YEAR(current_fecha)
            );

            SET current_fecha = DATE_ADD(current_fecha, INTERVAL 1 DAY);
        END WHILE;
    END WHILE;
END //

DELIMITER ;

-- Executem el procediment per carregar les dades.
CALL LoadDimFecha();

-- Creem la taula de fets
CREATE TABLE IF NOT EXISTS Fact_transactions (
	id VARCHAR(50) PRIMARY KEY,
    card_id VARCHAR(20),
    business_id VARCHAR(20), 
    timestamp DATETIME,
    amount DECIMAL(10,2),
    declined BOOLEAN,
    product_ids VARCHAR(20),
    user_id INT,
    lat FLOAT,
    longitude FLOAT,
    fecha_id DATE,
    FOREIGN KEY (user_id) REFERENCES Dim_users(id),
    FOREIGN KEY (business_id) REFERENCES Dim_companies(company_id),
    FOREIGN KEY (card_id) REFERENCES Dim_credit_cards(id),
    FOREIGN KEY (fecha_id) REFERENCES Dim_fecha(fecha)
);

-- Carreguem la taula transactions.csv

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/transactions.csv'
INTO TABLE fact_transactions
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(id, card_id, business_id, timestamp, amount, declined, product_ids, user_id, lat, longitude, @ignore);

-- afegim les dades a la columna fecha_id de fact_transactions amb la data de Timestamp i que estigui a la taula Dim_fecha

UPDATE Fact_transactions
SET fecha_id = DATE(TIMESTAMP)
WHERE DATE(TIMESTAMP) IN (SELECT fecha FROM Dim_fecha);

/* SPRINT 4
 Nivell 1 - Exercici 1
 Realitza una subconsulta que mostri tots els usuaris amb més de 30 transaccions utilitzant almenys 2 taules.
 */
 -- Sense rebutjar les declined
SELECT f.user_id, COUNT(f.id) AS Num_transaccions, (SELECT name FROM dim_users WHERE id = f.user_id) AS Nom_usuari
FROM fact_transactions AS f
GROUP BY user_id
HAVING Num_transaccions >30
ORDER BY Num_transaccions DESC;
  
 -- Rebutjant les declined
SELECT f.user_id, COUNT(f.id) AS Num_transaccions, (SELECT name FROM dim_users WHERE id = f.user_id) AS Nom_usuari
FROM fact_transactions AS f
WHERE f.declined = 0
GROUP BY user_id
HAVING Num_transaccions >30
ORDER BY Num_transaccions DESC;

/* SPRINT 4
 Nivell 1 - Exercici 2
Mostra la mitjana d'amount per IBAN de les targetes de crèdit a la companyia Donec Ltd, utilitza almenys 2 taules.
*/
-- Sense rebutjar les declined
SELECT c.iban, c.pan, AVG(t.amount) AS mitjana
FROM dim_credit_cards AS c
JOIN fact_transactions AS t
ON c.id = t.card_id
WHERE t.business_id IN (
		SELECT company_id 
		FROM dim_companies
		WHERE company_name ='Donec Ltd')
GROUP BY c.iban;

-- Rebutjan les declined
SELECT c.iban, c.pan, AVG(t.amount) AS mitjana 
FROM dim_credit_cards AS c
JOIN fact_transactions AS t
ON c.id = t.card_id
WHERE	t.business_id IN 
				(SELECT company_id
				FROM dim_companies
				WHERE company_name = 'Donec Ltd')
		AND t.declined = 0
GROUP BY c.iban;

/* SPRINT 4
 Nivell 2 - Exercici 1
 Crea una nova taula que reflecteixi l'estat de les targetes de crèdit basat en si les últimes tres transaccions van ser declinades i genera la següent consulta:
 
*/
-- Primer creem la taula
CREATE TABLE IF NOT EXISTS Dim_estat_targetes (
	card_id VARCHAR(20) PRIMARY KEY,
    estat VARCHAR(10)); -- aquí serà activa o inactiva, farem un CASE

-- S'ha de connectar amb Dim_credit_cards: afegir FK
ALTER TABLE Dim_credit_cards
ADD CONSTRAINT FK_dim_estat_targetes
FOREIGN KEY (id) REFERENCES dim_estat_targetes (card_id); 

-- Afegim les dades a la nova taula
-- Farem servir el case i una subquery
INSERT INTO Dim_estat_targetes (card_id, estat)
SELECT card_id,
	CASE
		WHEN COUNT(*) = 3 AND SUM(declined) = 3 THEN 'inactiva'
        ELSE 'activa'
	END AS estat
FROM (
	SELECT 
			card_id,
            declined,
            -- això dona un número de linia per cada transacció en ordre descent que ens fa falta per buscar les 3
            ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) AS vegades   
	FROM Fact_transactions) AS taula_derivada
WHERE vegades <= 3
GROUP BY card_id;

-- Quantes targetes estan actives?
SELECT COUNT(*) AS numero_targetes_actives
FROM Dim_estat_targetes
WHERE estat = 'activa';

/* SPRINT 4
Nivell 3 - Exercici 1
Crea una taula amb la qual puguem unir les dades del nou arxiu products.csv amb la base de dades creada, tenint en compte que des de transaction tens product_ids. Generar la següent consulta:

Necessitem conèixer el nombre de vegades que s'ha venut cada producte.
*/
-- Creem una taula intermitja per poder treballar amb els product_ids i els transaction_id. 
CREATE TABLE IF NOT EXISTS fact_transactions_products (
    transaction_id VARCHAR(50),
    product_id INT,
    FOREIGN KEY (transaction_id) REFERENCES fact_transactions(id),
    FOREIGN KEY (product_id) REFERENCES dim_products(id),
    PRIMARY KEY (transaction_id, product_id)
);

-- Creació de taula temporal per poder afegir la columna amb els product_ids i separar-los.
CREATE TEMPORARY TABLE temp_transactions_products (
    transaction_id VARCHAR(50),
    product_ids VARCHAR(50)
);

-- Carreguem la taula temporal només amb columnes que interessen de transactions.csv

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/transactions.csv'
INTO TABLE temp_transactions_products
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(transaction_id, @ignore, @ignore, @ignore, @ignore, @ignore, @product_ids_temp, @ignore, @ignore, @ignore)
SET	product_ids = REPLACE(@product_ids_temp, ' ', '');

-- Creem un altre procediment per dividir els ids i alimentar la taula
DELIMITER //

CREATE PROCEDURE split_and_insert_product_ids()
BEGIN
    DECLARE finished INT DEFAULT 0;
    DECLARE current_transaction_id VARCHAR(50);
    DECLARE current_product_ids VARCHAR(50);
    DECLARE single_product_id VARCHAR(50);
    DECLARE product_pos INT;

    DECLARE temp_cursor CURSOR FOR SELECT transaction_id, product_ids FROM temp_transactions_products;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished = 1;

    OPEN temp_cursor;

    read_loop: LOOP
        FETCH temp_cursor INTO current_transaction_id, current_product_ids;
        IF finished THEN
            LEAVE read_loop;
        END IF;

        WHILE LENGTH(current_product_ids) > 0 DO
            SET product_pos = LOCATE(',', current_product_ids);

            IF product_pos = 0 THEN
                SET single_product_id = current_product_ids;
                SET current_product_ids = '';
            ELSE
                SET single_product_id = LEFT(current_product_ids, product_pos - 1);
                SET current_product_ids = SUBSTRING(current_product_ids, product_pos + 1);
            END IF;

            INSERT INTO fact_transactions_products (transaction_id, product_id)
            VALUES (current_transaction_id, single_product_id);
        END WHILE;
    END LOOP;

    CLOSE temp_cursor;
END //

DELIMITER ;

-- Executar el procediment per dividir-ho i carregar-ho
CALL split_and_insert_product_ids();

-- Consulta per conèixer el nombre de vegades que s'ha venut cada producte
SELECT 	p.id AS product_id,
		p.product_name,
        COUNT(tp.transaction_id) AS nombre_vegades_venut
FROM Dim_products AS p
LEFT JOIN Fact_transactions_products AS tp
ON p.id = tp.product_id
GROUP BY p.id, p.product_name
ORDER BY nombre_vegades_venut DESC;