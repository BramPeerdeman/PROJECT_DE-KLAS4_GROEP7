-- =============================================================
-- MASTER SQL SCRIPT – GreatOutdoors SDM
-- Tabellen worden aangemaakt in volgorde van afhankelijkheid
-- (eerst parent-tabellen, dan child-tabellen)
-- Voer uit met: PRAGMA foreign_keys = ON;
-- =============================================================

PRAGMA foreign_keys = ON;

-- =============================================================
-- STAP 1 – TABELLEN ZONDER AFHANKELIJKHEDEN (geen FK's)
-- =============================================================

CREATE TABLE IF NOT EXISTS country (
    country_code    INTEGER PRIMARY KEY,
    country         TEXT,
    language        TEXT,
    currency_name   TEXT
);

CREATE TABLE IF NOT EXISTS order_method (
    order_method_code   INTEGER PRIMARY KEY,
    order_method_en     TEXT
);

CREATE TABLE IF NOT EXISTS product_line (
    product_line_code   INTEGER PRIMARY KEY,
    product_line_en     TEXT
);

CREATE TABLE IF NOT EXISTS return_reason (
    return_reason_code      INTEGER PRIMARY KEY,
    return_description_en   TEXT
);

CREATE TABLE IF NOT EXISTS sales_territory (
    SALES_TERRITORY_CODE    INTEGER PRIMARY KEY,
    TERRITORY_NAME_EN       TEXT
);

CREATE TABLE IF NOT EXISTS age_group (
    AGE_GROUP_CODE  INTEGER PRIMARY KEY,
    UPPER_AGE       INTEGER,
    LOWER_AGE       INTEGER
);

CREATE TABLE IF NOT EXISTS customer_type (
    CUSTOMER_TYPE_CODE  INTEGER PRIMARY KEY,
    CUSTOMER_TYPE_EN    TEXT
);

CREATE TABLE IF NOT EXISTS customer_segment (
    SEGMENT_CODE        INTEGER PRIMARY KEY,
    LANGUAGE            TEXT,
    SEGMENT_NAME        TEXT,
    SEGMENT_DESCRIPTON  TEXT
);

CREATE TABLE IF NOT EXISTS course (
    COURSE_CODE         INTEGER PRIMARY KEY,
    COURSE_DESCRIPTION  TEXT
);

CREATE TABLE IF NOT EXISTS satisfaction_type (
    SATISFACTION_TYPE_CODE          INTEGER PRIMARY KEY,
    SATISFACTION_TYPE_DESCRIPTION   TEXT
);

-- =============================================================
-- STAP 2 – TABELLEN MET ÉÉN NIVEAU AFHANKELIJKHEDEN
-- =============================================================

CREATE TABLE IF NOT EXISTS sales_branch (
    sales_branch_code   INTEGER PRIMARY KEY,
    address1            TEXT,
    address2            TEXT,
    city                TEXT,
    region              TEXT,
    postal_zone         TEXT,
    country_code        INTEGER,
    FOREIGN KEY (country_code) REFERENCES country (country_code)
);

CREATE TABLE IF NOT EXISTS crm_country (
    COUNTRY_CODE            INTEGER PRIMARY KEY,
    COUNTRY_EN              TEXT,
    FLAG_IMAGE              TEXT,
    SALES_TERRITORY_CODE    INTEGER,
    FOREIGN KEY (SALES_TERRITORY_CODE) REFERENCES sales_territory (SALES_TERRITORY_CODE)
);

CREATE TABLE IF NOT EXISTS product_type (
    product_type_code   INTEGER PRIMARY KEY,
    product_line_code   INTEGER,
    product_type_en     TEXT,
    FOREIGN KEY (product_line_code) REFERENCES product_line (product_line_code)
);

CREATE TABLE IF NOT EXISTS sales_office (
    SALES_OFFICE_CODE   INTEGER PRIMARY KEY,
    STREET              TEXT,
    ADDITION            TEXT,
    CITY                TEXT,
    REGION              TEXT,
    ZIPCODE             TEXT,
    COUNTRY_CODE        INTEGER,
    FOREIGN KEY (COUNTRY_CODE) REFERENCES crm_country (COUNTRY_CODE)
);

-- =============================================================
-- STAP 3 – TABELLEN MET TWEE NIVEAUS AFHANKELIJKHEDEN
-- =============================================================

CREATE TABLE IF NOT EXISTS sales_staff (
    sales_staff_code    INTEGER PRIMARY KEY,
    first_name          TEXT,
    last_name           TEXT,
    address2            TEXT,
    position_en         TEXT,
    work_phone          TEXT,
    extension           TEXT,
    fax                 TEXT,
    email               TEXT,
    date_hired          TEXT,
    sales_branch_code   INTEGER,
    FOREIGN KEY (sales_branch_code) REFERENCES sales_branch (sales_branch_code)
);

CREATE TABLE IF NOT EXISTS retailer_site (
    retailer_site_code  INTEGER PRIMARY KEY,
    retailer_code       INTEGER,
    item_3              TEXT,
    address1            TEXT,
    address2            TEXT,
    city                TEXT,
    region              TEXT,
    postal_zone         TEXT,
    country_code        INTEGER,
    active_indicator    INTEGER,
    FOREIGN KEY (country_code) REFERENCES country (country_code)
);

CREATE TABLE IF NOT EXISTS product (
    product_number      INTEGER PRIMARY KEY,
    introduction_date   TEXT,
    product_type_code   INTEGER,
    product_type_cost   REAL,
    margin              REAL,
    product_image       TEXT,
    language            TEXT,
    product_name        TEXT,
    description         TEXT,
    FOREIGN KEY (product_type_code) REFERENCES product_type (product_type_code)
);

CREATE TABLE IF NOT EXISTS customer_headquarters (
    CUSTOMER_CODEMR INTEGER PRIMARY KEY,
    CUSTOMER_NAME   TEXT,
    ADDRESS1        TEXT,
    ADDRESS2        TEXT,
    CITY            TEXT,
    REGION          TEXT,
    POSTAL_ZONE     TEXT,
    COUNTRY_CODE    INTEGER,
    PHONE           TEXT,
    FAX             TEXT,
    SEGMENT_CODE    INTEGER,
    FOREIGN KEY (COUNTRY_CODE)   REFERENCES crm_country (COUNTRY_CODE),
    FOREIGN KEY (SEGMENT_CODE)   REFERENCES customer_segment (SEGMENT_CODE)
);

CREATE TABLE IF NOT EXISTS sales_representative (
    SALES_REPRESENTATIVE_CODE   INTEGER PRIMARY KEY,
    FIRST_NAME                  TEXT,
    LAST_NAME                   TEXT,
    POSITION_EN                 TEXT,
    WORK_PHONE                  TEXT,
    EXTENSION                   TEXT,
    FAX                         TEXT,
    EMAIL                       TEXT,
    DATE_HIRED                  TEXT,
    SALES_OFFICE_CODE           INTEGER,
    MANAGER_CODE                INTEGER,
    FOREIGN KEY (SALES_OFFICE_CODE) REFERENCES sales_office (SALES_OFFICE_CODE)
);

-- =============================================================
-- STAP 4 – TRANSACTIE- EN KOPPELTABELLEN
-- =============================================================

CREATE TABLE IF NOT EXISTS order_header (
    order_number            INTEGER PRIMARY KEY,
    retailer_name           TEXT,
    retailer_site_code      INTEGER,
    retailer_contact_code   INTEGER,
    sales_staff_code        INTEGER,
    sales_branch_code       INTEGER,
    order_date              TEXT,
    order_method_code       INTEGER,
    FOREIGN KEY (retailer_site_code)    REFERENCES retailer_site (retailer_site_code),
    FOREIGN KEY (sales_staff_code)      REFERENCES sales_staff (sales_staff_code),
    FOREIGN KEY (sales_branch_code)     REFERENCES sales_branch (sales_branch_code),
    FOREIGN KEY (order_method_code)     REFERENCES order_method (order_method_code)
);

CREATE TABLE IF NOT EXISTS customer (
    CUSTOMER_CODE       INTEGER PRIMARY KEY,
    CUSTOMER_CODEMR     INTEGER,
    COMPANY_NAME        TEXT,
    CUSTOMER_TYPE_CODE  INTEGER,
    FOREIGN KEY (CUSTOMER_CODEMR)       REFERENCES customer_headquarters (CUSTOMER_CODEMR),
    FOREIGN KEY (CUSTOMER_TYPE_CODE)    REFERENCES customer_type (CUSTOMER_TYPE_CODE)
);

CREATE TABLE IF NOT EXISTS customer_store (
    CUSTOMER_SITE_CODE  INTEGER PRIMARY KEY,
    CUSTOMER_CODE       INTEGER,
    STREET              TEXT,
    ADDITION            TEXT,
    CITY                TEXT,
    STATE               TEXT,
    ZIPCODE             TEXT,
    COUNTRY_CODE        INTEGER,
    ACTIVE_INDICATOR    INTEGER,
    FOREIGN KEY (CUSTOMER_CODE)  REFERENCES customer (CUSTOMER_CODE),
    FOREIGN KEY (COUNTRY_CODE)   REFERENCES crm_country (COUNTRY_CODE)
);

CREATE TABLE IF NOT EXISTS sales_demographic (
    DEMOGRAPHIC_CODE    INTEGER PRIMARY KEY,
    CUSTOMER_CODEMR     INTEGER,
    AGE_GROUP_CODE      INTEGER,
    SALES_PERCENT       INTEGER,
    FOREIGN KEY (CUSTOMER_CODEMR)   REFERENCES customer_headquarters (CUSTOMER_CODEMR),
    FOREIGN KEY (AGE_GROUP_CODE)    REFERENCES age_group (AGE_GROUP_CODE)
);

CREATE TABLE IF NOT EXISTS training (
    YEAR                        INTEGER,
    SALES_REPRESENTATIVE_CODE   INTEGER,
    COURSE_CODE                 INTEGER,
    PRIMARY KEY (YEAR, SALES_REPRESENTATIVE_CODE, COURSE_CODE),
    FOREIGN KEY (SALES_REPRESENTATIVE_CODE) REFERENCES sales_representative (SALES_REPRESENTATIVE_CODE),
    FOREIGN KEY (COURSE_CODE)               REFERENCES course (COURSE_CODE)
);

CREATE TABLE IF NOT EXISTS satisfaction (
    YEAR                        INTEGER,
    SALES_REPRESENTATIVE_CODE   INTEGER,
    SATISFACTION_TYPE_CODE      INTEGER,
    PRIMARY KEY (YEAR, SALES_REPRESENTATIVE_CODE, SATISFACTION_TYPE_CODE),
    FOREIGN KEY (SALES_REPRESENTATIVE_CODE) REFERENCES sales_representative (SALES_REPRESENTATIVE_CODE),
    FOREIGN KEY (SATISFACTION_TYPE_CODE)    REFERENCES satisfaction_type (SATISFACTION_TYPE_CODE)
);

-- =============================================================
-- STAP 5 – DETAIL- EN FEITTABELLEN
-- =============================================================

CREATE TABLE IF NOT EXISTS order_details (
    order_detail_code   INTEGER PRIMARY KEY,
    order_number        INTEGER,
    product_number      INTEGER,
    quantity            INTEGER,
    unit_cost           REAL,
    unit_price          REAL,
    unit_sale_price     REAL,
    FOREIGN KEY (order_number)   REFERENCES order_header (order_number),
    FOREIGN KEY (product_number) REFERENCES product (product_number)
);

CREATE TABLE IF NOT EXISTS returned_item (
    return_code         INTEGER PRIMARY KEY,
    return_date         TEXT,
    order_detail_code   INTEGER,
    return_reason_code  INTEGER,
    return_quantity     INTEGER,
    FOREIGN KEY (order_detail_code)  REFERENCES order_details (order_detail_code),
    FOREIGN KEY (return_reason_code) REFERENCES return_reason (return_reason_code)
);

CREATE TABLE IF NOT EXISTS customer_contact (
    CUSTOMER_CONTACT_CODE   INTEGER PRIMARY KEY,
    CUSTOMER_SITE_CODE      INTEGER,
    FIRST_NAME              TEXT,
    LAST_NAME               TEXT,
    JOB_POSITION_EN         TEXT,
    FAX                     TEXT,
    E_MAIL                  TEXT,
    GENDER                  TEXT,
    FOREIGN KEY (CUSTOMER_SITE_CODE) REFERENCES customer_store (CUSTOMER_SITE_CODE)
);

CREATE TABLE IF NOT EXISTS sales_target (
    SALES_TARGET_ID     INTEGER PRIMARY KEY AUTOINCREMENT,
    SALES_STAFF_CODE    INTEGER,
    SALES_YEAR          INTEGER,
    SALES_PERIOD        INTEGER,
    RETAILER_NAME       TEXT,
    PRODUCT_NUMBER      INTEGER,
    SALES_TARGET        INTEGER,
    RETAILER_CODE       INTEGER,
    FOREIGN KEY (SALES_STAFF_CODE) REFERENCES sales_staff (sales_staff_code),
    FOREIGN KEY (PRODUCT_NUMBER)   REFERENCES product (product_number)
);

CREATE TABLE IF NOT EXISTS product_forecast (
    PRODUCT_NUMBER  INTEGER,
    YEAR            INTEGER,
    MONTH           INTEGER,
    EXPECTED_VOLUME INTEGER,
    PRIMARY KEY (PRODUCT_NUMBER, YEAR, MONTH),
    FOREIGN KEY (PRODUCT_NUMBER) REFERENCES product (product_number)
);

CREATE TABLE IF NOT EXISTS inventory_levels (
    INVENTORY_YEAR  INTEGER,
    INVENTORY_MONTH INTEGER,
    PRODUCT_NUMBER  INTEGER,
    INVENTORY_COUNT INTEGER,
    PRIMARY KEY (INVENTORY_YEAR, INVENTORY_MONTH, PRODUCT_NUMBER),
    FOREIGN KEY (PRODUCT_NUMBER) REFERENCES product (product_number)
);