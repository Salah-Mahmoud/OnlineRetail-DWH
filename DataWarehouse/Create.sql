-- Customer Dimension Table
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

-- Product Dimension Table
CREATE TABLE ProductDim (
  ProductID SERIAL PRIMARY KEY,  
  ProductName VARCHAR(100) NOT NULL UNIQUE,  
  Category VARCHAR(50),
  Price DECIMAL(10, 2),
  PreviousPrice DECIMAL(10, 2)
);

-- Date Dimension Table
CREATE TABLE DateDim (
  TimeID SERIAL PRIMARY KEY,  
  Date DATE UNIQUE NOT NULL,  
  DayOfWeek VARCHAR(15),
  Month VARCHAR(9),     
  Quarter CHAR(2),  
  Year INT,          
  IsHoliday BOOLEAN DEFAULT FALSE  
);

-- Store Dimension Table
CREATE TABLE StoreDim (
  StoreID SERIAL PRIMARY KEY,  
  StoreName VARCHAR(50) NOT NULL UNIQUE,  
  StoreLocation VARCHAR(100),
  StoreType VARCHAR(50),
  OpeningDate DATE,
  ManagerName VARCHAR(50)
);

-- Payment Method Dimension Table
CREATE TABLE PaymentMethodDim (
  PaymentMethodID SERIAL PRIMARY KEY, 
  PaymentType VARCHAR(50) NOT NULL UNIQUE,  
  Provider VARCHAR(50)
);

-- Promotion Dimension Table
CREATE TABLE PromotionDim (
  PromotionID SERIAL PRIMARY KEY, 
  PromotionName VARCHAR(100) NOT NULL UNIQUE, 
  DiscountPercentage DECIMAL(5, 2),
  PromotionType VARCHAR(50),
  IsActivePromotion BOOLEAN DEFAULT TRUE  
);

-- Sales Fact Table
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

