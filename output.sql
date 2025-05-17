CREATE TABLE IF NOT EXISTS users (
    `INDEX___A` TINYINT,
    `CUSTOMER_ID` VARCHAR(25),
    `FIRST_NAME` VARCHAR(20),
    `LAST_NAME` VARCHAR(21),
    `COMPANY` VARCHAR(41),
    `CITY` VARCHAR(29),
    `COUNTRY` VARCHAR(54),
    `PHONE_1` VARCHAR(32),
    `PHONE_2` VARCHAR(32),
    `EMAIL` VARCHAR(44),
    `SUBSCRIPTION_DATE` VARCHAR(20),
    `WEBSITE` VARCHAR(43)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO users (`INDEX___A`, `CUSTOMER_ID`, `GIVEN_NAME`, `SURNAME`, `COMPANY`, `CITY`, `COUNTRY`, `PRIMARY_PHONE`, `SECONDARY_PHONE`, `EMAIL`, `SUBSCRIPTION_DATE`, `WEBSITE`, `FULL_NAME`, `SUBSCRIPTION_AGE_DAYS`, `EMAIL_DOMAIN`) VALUES (70, 'CC68FD1D3Bbbf22', 'Riley', 'Good', 'Wade PLC', 'Erikaville', 'Canada', '6977745822', '855-436-7641', 'alex06@galloway.com', '2020-02-03', 'http://conway.org/', 'Riley Good', 1930, 'galloway.com');
INSERT INTO users (`INDEX___A`, `CUSTOMER_ID`, `GIVEN_NAME`, `SURNAME`, `COMPANY`, `CITY`, `COUNTRY`, `PRIMARY_PHONE`, `SECONDARY_PHONE`, `EMAIL`, `SUBSCRIPTION_DATE`, `WEBSITE`, `FULL_NAME`, `SUBSCRIPTION_AGE_DAYS`, `EMAIL_DOMAIN`) VALUES (51, 'Aa20BDe68eAb0e9', 'Gerald', 'Hawkins', 'Phelps, Forbes and Koch', 'New Alberttown', 'Canada', '+1-323-239-1456x96168', '(092)508-0269', 'uwarner@steele-arias.com', '2021-03-19', 'https://valenzuela.com/', 'Gerald Hawkins', 1520, 'steele-arias.com');
