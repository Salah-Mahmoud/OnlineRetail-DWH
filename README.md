
# Data Warehouse ETL Pipeline Using Apache Spark, Airflow, and PostgreSQL

In this project, I built a containerized ETL pipeline that integrates Apache Spark, Apache Airflow, and PostgreSQL. The primary goal is to extract, transform, and load (ETL) data into a star schema data warehouse to enable efficient querying and analysis of business data, focusing on an **online retail dataset**.

## Table of Contents
- [Architecture](#architecture)
- [Data Warehouse Schema](#data-warehouse-schema)
- [Setup Instructions](#setup-instructions)
- [Airflow DAG](#airflow-dag)
- [Sample Data and Output](#sample-data-and-output)
- [Future Enhancements](#future-enhancements)

## Architecture

- **Apache Spark**: Used for distributed data processing and transformations.
- **Apache Airflow**: Orchestrates the ETL process.
- **PostgreSQL**: Stores the transformed data in the form of a data warehouse.
- **Docker Compose**: Manages container orchestration for all services.

![tools](tools.png)

## Data Warehouse Schema

The schema follows a star schema structure with dimension and fact tables.

### Customer Dimension Table
```sql
CREATE TABLE CustomerDim (
  CustomerID SERIAL PRIMARY KEY,  
  Name VARCHAR(100) NOT NULL,
  Email VARCHAR(100) UNIQUE NOT NULL, 
  PhoneNumber VARCHAR(100),
  Address VARCHAR(255),
  City VARCHAR(100),
  State VARCHAR(100),
  Country VARCHAR(100),
  ZipCode VARCHAR(20),
  PreviousCity VARCHAR(100)
);
```

### Product Dimension Table
```sql
CREATE TABLE ProductDim (
  ProductID SERIAL PRIMARY KEY,  
  ProductName VARCHAR(100) NOT NULL UNIQUE,  
  Category VARCHAR(50),
  Price DECIMAL(10, 2),
  PreviousPrice DECIMAL(10, 2)
);
```

### Date Dimension Table
```sql
CREATE TABLE DateDim (
  TimeID SERIAL PRIMARY KEY,  
  Date DATE UNIQUE NOT NULL,  
  DayOfWeek VARCHAR(15),
  Month VARCHAR(9),     
  Quarter CHAR(2),  
  Year INT,          
  IsHoliday BOOLEAN DEFAULT FALSE  
);
```

### Store Dimension Table
```sql
CREATE TABLE StoreDim (
  StoreID SERIAL PRIMARY KEY,  
  StoreName VARCHAR(50) NOT NULL UNIQUE,  
  StoreLocation VARCHAR(100),
  StoreType VARCHAR(50),
  OpeningDate DATE,
  ManagerName VARCHAR(50)
);
```

### Sales Fact Table
```sql
CREATE TABLE SalesFact (
  SalesID SERIAL PRIMARY KEY,  
  CustomerID INT,
  ProductID INT,
  TimeID INT,
  StoreID INT,
  PaymentMethodID INT,
  PromotionID INT,
  QuantitySold INT,
  TotalSales DECIMAL(10, 2),
  DiscountAmount DECIMAL(10, 2),
  NetSales DECIMAL(10, 2),
  FOREIGN KEY (CustomerID) REFERENCES CustomerDim(CustomerID),
  FOREIGN KEY (ProductID) REFERENCES ProductDim(ProductID),
  FOREIGN KEY (TimeID) REFERENCES DateDim(TimeID),
  FOREIGN KEY (StoreID) REFERENCES StoreDim(StoreID),
  FOREIGN KEY (PaymentMethodID) REFERENCES PaymentMethodDim(PaymentMethodID),
  FOREIGN KEY (PromotionID) REFERENCES PromotionDim(PromotionID)
);
```

![DWH](DwhOnlineRetail.png)


## Setup Instructions

### Prerequisites
- Docker installed
- Docker Compose installed

### Steps to Run

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/Salah-Mahmoud/OnlineRetail-DWH.git
   cd OnlineRetail-DWH
   ```

2. **Run Docker Compose**:
   ```bash
   docker-compose up
   ```

3. **Access the Services**:
   - **Airflow Webserver**: [http://localhost:8080](http://localhost:8080)
   - **PgAdmin**: [http://localhost:5050](http://localhost:5050)
   - **Spark Master UI**: [http://localhost:9090](http://localhost:9090)

### Important Directories
- **`jobs/`**: Contains the PySpark scripts for transformations.
- **`data/`**: Source CSV data files.
- **`dags/`**: Airflow DAGs that manage the ETL process.

## Airflow DAG

The Airflow Directed Acyclic Graph (DAG) orchestrates the entire ETL process:
- **Truncate staging tables**: Clears the staging area in the database.
- **Stage data**: Extracts data from CSV files and stages it in the PostgreSQL database.
- **Transformations**: Python tasks transform the data into structured tables like `CustomerDim`, `ProductDim`, and `SalesFact`.
- **Load**: The final transformed data is loaded into the PostgreSQL data warehouse, populating the dimension and fact tables. This prepares the data for querying and analysis.

## Sample Data and Output

### Sample Input Data

Below is a sample of the input data used for the ETL process:

|Below is a sample of the input data used for the ETL process:

| FirstName | LastName | Email                    | PhoneNumber         | Address                                               | City         | State   | Country              | ZipCode | PreviousCity   | ProductName        | Category      | Price   | PreviousPrice | PurchaseDate | StoreName                 | StoreLocation                                    | StoreType   | OpeningDate | ManagerName      | PaymentType  | Provider  | PromotionName                         | DiscountPercentage | PromotionType | IsActivePromotion | QuantitySold | TotalSales | DiscountAmount | NetSales |
|-----------|----------|--------------------------|---------------------|-------------------------------------------------------|--------------|---------|----------------------|---------|----------------|--------------------|---------------|---------|---------------|--------------|--------------------------|-------------------------------------------------|-------------|-------------|------------------|--------------|-----------|----------------------------------------|-------------------|---------------|------------------|--------------|------------|----------------|-----------|
| Benjamin  | Lawson   | maria89@example.net       | 001-804-739-4570x167| 00828 Young Walk Apt. 060, Harrisburgh, OR 64222       | Port Daniel  | Maine   | Antigua and Barbuda   | 84534   | Marshallland   | huge (New)          | throughout    | $510.98 | $1.48         | 2021-11-14   | Page-Flores Inc.          | 061 Mark Creek, Aaronport, IL 82900             | Boutique    | 2024-01-10  | Angel Fernandez  | debit card   | Maestro   | Implemented full-range time-frame!     | 30.24             | Seasonal      | 1                | 5 units      | $2554.9    | $772.6         | $1782.3   |

### Output and Results

Once the ETL pipeline is successfully executed, you can query the data warehouse to analyze business data. Below are some sample outputs from the data warehouse:

#### Sales Fact Table:
| salesid | customerid | productid | timeid | storeid | paymentmethodid | promotionid | quantitysold | totalsales | discountamount | netsales |
|---------|------------|-----------|--------|---------|-----------------|-------------|--------------|------------|----------------|----------|
| 1000001 | 480364     | 5866      | 2532   | 605503  | 6               | 444917      | 5           | 2554.9    | 772.6        | 1782.3  |

#### Customer Dimension Table:
| customerid | name             | email                | phonenumber  | address                    | city           | state         | country     | zipcode | previouscity |
|------------|------------------|----------------------|--------------|----------------------------|----------------|---------------|-------------|---------|--------------|
| 480364     | Benjamin Lawson   | maria89@example.net | 001-804-739-4570x167   | 00828 Young Walk Apt. 060, Harrisburgh, OR 64222        | Port Daniel    | Maine | Antigua and Barbuda      | 84534   | Marshallland   |

#### Time Dimension Table:
| timeid | date       | dayofweek | month   | quarter | year | isholiday |
|--------|------------|-----------|---------|---------|------|-----------|
| 2532   | 2021-11-14 | Wednesday | Nov | 4      | 2021 | False     |


### Store Dimension Table:
| Store ID | Store Name          | Location                           | Manager         | Open Date  | Store Type |
|----------|---------------------|------------------------------------|-----------------|------------|------------|
| 605503   | Page-Flores Inc.     | 061 Mark Creek, Aaronport, IL 82900| Angel Fernandez | 2024-01-10 | Boutique   |


#### Payment Method Dimension Table:
| paymentmethodid | paymenttype   | provider   |
|-----------------|---------------|------------|
| 6               | debit card  | Maestro       |

#### Product Dimension Table:
| productid | productname     | category  | price   | previousprice |
|-----------|-----------------|-----------|---------|---------------|
| 5866      | American         | improve   | 587.08  | 2.25          |

#### Promotion Dimension Table:
| promotionid | promotionname                   | discountpercentage | promotiontype    | isactivepromotion |
|-------------|---------------------------------|--------------------|------------------|-------------------|
| 444917      | Implemented full-range time-frame!    | 30.24      | Seasonal  | True                       |

## Future Enhancements

- **Automate Data Quality Checks**: Add checks to ensure data consistency.
- **Include Monitoring and Alerts**: Integrate with Airflow to send alerts if a task fails.
- **Data Partitioning**: Partition large datasets for better performance.
