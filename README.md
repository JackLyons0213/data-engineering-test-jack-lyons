# README

## **Introduction**
This project demonstrates a data pipeline designed to process a dataset of vehicle sales. The pipeline includes data extraction, validation, transformation, and loading the processed data into a mock data warehouse in both CSV and SQLite formats. This ensures efficient and structured handling of sales data for further analytics and reporting.

---

## **Objective**
The objective of this project is to build a robust data pipeline that:
1. Extracts data from a CSV file.
2. Validates the dataset to ensure the presence of required fields and handles missing or invalid values.
3. Transforms the data by converting dates to a standard format and aggregating sales data by category (make and model).
4. Loads the transformed data into a mock data warehouse, saving results to both CSV files and an SQLite database.

---

## **Dataset**
The dataset contains details of vehicle sales, including:
- **Columns**:
  - `year`: Year of manufacture.
  - `make`: Vehicle make (e.g., Kia, BMW).
  - `model`: Vehicle model.
  - `trim`: Vehicle trim level.
  - `body`: Vehicle body type (e.g., Sedan, SUV).
  - `vin`: Vehicle Identification Number.
  - `state`: State of sale.
  - `odometer`: Odometer reading at the time of sale.
  - `color`: Exterior color of the vehicle.
  - `interior`: Interior color of the vehicle.
  - `seller`: Name of the seller.
  - `mmr`: Manheim Market Report value.
  - `sellingprice`: Final selling price.
  - `saledate`: Date of the sale.

- **Sample Dataset**: The dataset is provided as a CSV file (`car_prices.csv`) which is downloaded from [Kaggle.com](https://www.kaggle.com/datasets/syedanwarafridi/vehicle-sales-data). A few sample rows are:
  ```csv
  year,make,model,trim,body,vin,state,odometer,color,interior,seller,mmr,sellingprice,saledate
  2015,Kia,Sorento,LX,SUV,5xyktca69fg566472,ca,16639,white,black,kia motors america inc,20500,21500,Tue Dec 16 2014 12:30:00 GMT-0800 (PST)
  2014,BMW,3 Series,328i SULEV,Sedan,wba3c1c51ek116351,ca,1331,gray,black,financial services remarketing (lease),31900,30000,Thu Jan 15 2015 04:30:00 GMT-0800 (PST)
  ```

---

## **How to Use**

### **Setup**
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Install required Python packages:
   ```bash
   pip install pandas
   ```

### **Run the Pipeline**
1. Ensure the raw dataset (`car_prices.csv`) is placed in the `data/` directory.
2. Execute the pipeline script:
   ```bash
   python src/data_pipeline.py
   ```

### **Output**
- The pipeline generates the following outputs:
  1. **Converted Data**:
     - CSV: `processed_data/converted_car_prices.csv`
     - SQLite Table: `converted_date_format` in `car_prices.db`
  2. **Aggregated Data**:
     - CSV: `processed_data/aggregated_car_prices.csv`
     - SQLite Table: `car_summary` in `car_prices.db`

---

## **Testing**
Run the unit tests to verify the pipeline functionality:
```bash
python -m unittest tests/test_data_pipeline.py
```

---

## **Pipeline Workflow**
1. **Extraction**:
   - Reads the raw dataset from a CSV file.
   - Handles any file read errors.
2. **Validation**:
   - Ensures all required columns are present.
   - Drops rows with missing or invalid data.
3. **Transformation**:
   - Converts `saledate` to a standard format.
   - Aggregates sales data by `make` and `model`, calculating average selling price and odometer reading.
4. **Loading**:
   - Saves transformed data to both CSV and SQLite formats.

---

## **Future Improvements**
- Enhance validation to include specific range checks (e.g., valid years or odometer values).
- Integrate logging for better traceability of pipeline operations.
- Extend transformation to include more advanced analytics, such as sales trends.
