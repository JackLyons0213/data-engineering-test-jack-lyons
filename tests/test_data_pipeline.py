import sys
import os
import unittest
import pandas as pd
from io import StringIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from data_pipeline import (
    extract_data,
    validate_data,
    convert_dates_to_standard_format,
    aggregate_sales_by_category,
)


class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        self.mock_csv = """year,make,model,trim,body,vin,state,odometer,color,interior,seller,mmr,sellingprice,saledate
2015,Kia,Sorento,LX,SUV,5xyktca69fg566472,ca,16639,white,black,kia motors america inc,20500,21500,Tue Dec 16 2014 12:30:00 GMT-0800 (PST)
2015,Kia,Sorento,LX,SUV,5xyktca69fg561319,ca,9393,white,beige,kia motors america inc,20800,21500,Tue Dec 16 2014 12:30:00 GMT-0800 (PST)
2014,BMW,3 Series,328i SULEV,Sedan,wba3c1c51ek116351,ca,1331,gray,black,financial services remarketing (lease),31900,30000,Thu Jan 15 2015 04:30:00 GMT-0800 (PST)
"""
        self.df = pd.read_csv(StringIO(self.mock_csv))

    def test_extract_data(self):
        df = extract_data(StringIO(self.mock_csv))
        self.assertEqual(df.shape[0], 3)

    def test_validate_data(self):
        validated_df = validate_data(self.df)
        self.assertEqual(validated_df.shape[0], 3)

    def test_validate_data_missing_columns(self):
        df_missing_column = self.df.drop(columns=["make"])
        with self.assertRaises(ValueError):
            validate_data(df_missing_column)

    def test_convert_dates_to_standard_format(self):
        converted_df = convert_dates_to_standard_format(self.df)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(converted_df["saledate"]))
        self.assertEqual(converted_df.shape[0], 3)

    def test_convert_dates_to_standard_format_invalid_date(self):
        self.df.loc[0, "saledate"] = "invalid_date"
        converted_df = convert_dates_to_standard_format(self.df)
        self.assertEqual(
            converted_df.shape[0], 2
        )  # One row dropped due to invalid date

    def test_aggregate_sales_by_category(self):
        aggregated_df = aggregate_sales_by_category(self.df)
        self.assertIn("avg_sellingprice", aggregated_df.columns)
        self.assertIn("avg_odometer", aggregated_df.columns)
        self.assertEqual(
            aggregated_df.shape[0], 2
        )  # Two unique make-model combinations


if __name__ == "__main__":
    unittest.main()
