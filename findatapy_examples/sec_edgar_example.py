'''
In this example, we query SEC filings using the Edgar API provided by the SEC. 
We use the Python wrapper from the SEC-Edgar-API library (source: https://github.com/jadchaar/sec-edgar-api).
'''

import pandas as pd
from sec_edgar_api import EdgarClient
import datetime
import time
import matplotlib.pyplot as plt
from findatapy.market.datavendor import DataVendor
from findatapy.util import LoggerManager

class DataVendorSEC(DataVendor):
    """DataVendor class for fetching financial metrics from SEC Edgar API."""

    def __init__(self, user_agent="FindatapyExample"):
        super().__init__()
        self.logger = LoggerManager().getLogger(__name__)
        self.edgar = EdgarClient(user_agent=user_agent)
        self.TAXONOMY = "us-gaap"
        self.FINANCIAL_TAGS = [
            "AccountsPayableCurrent",
            "NetIncomeLoss",
            "Assets",
            "Liabilities",
            "Revenues",
            "CashAndCashEquivalentsAtCarryingValue"
        ]

    def fetch_financial_metric(self, cik, taxonomy=None, tag=None, retries=3):
        """Fetch a financial metric from the SEC Edgar API with retries."""
        taxonomy = taxonomy or self.TAXONOMY
        for attempt in range(retries):
            try:
                concept_data = self.edgar.get_company_concept(cik=cik, taxonomy=taxonomy, tag=tag)
                units = concept_data.get('units', {}).get('USD', [])
                if units:
                    df = pd.DataFrame(units)
                    df['end'] = pd.to_datetime(df['end'])
                    df = df.drop_duplicates(subset='end', keep='last')
                    df.set_index('end', inplace=True)
                    return df['val']
            except Exception as e:
                self.logger.warning(f"Error fetching {tag}: {e}. Attempt {attempt + 1}/{retries}")
                time.sleep(2 ** attempt)
        return pd.Series(dtype='float64')

    def load_financial_metrics(self, cik, taxonomy=None):
        """Load financial metrics for a given CIK."""
        metrics_data = {}
        taxonomy = taxonomy or self.TAXONOMY
        for tag in self.FINANCIAL_TAGS:
            series = self.fetch_financial_metric(cik, taxonomy, tag)
            if not series.empty:
                metrics_data[tag] = series
            else:
                self.logger.warning(f"No data found for tag: {tag}")
        return pd.DataFrame(metrics_data)

    def save_plots(self, financial_metrics, output_path="financial_metrics.pdf"):
        """Save the SEC financial data plots as a PDF."""
        if financial_metrics.empty:
            self.logger.warning("No financial metrics data available for plotting.")
            return

        plt.figure(figsize=(14, 7))
        for tag in ['Assets', 'Liabilities', 'Revenues']:
            if tag in financial_metrics:
                plt.plot(financial_metrics[tag].index, financial_metrics[tag], label=tag)
        if 'CashAndCashEquivalentsAtCarryingValue' in financial_metrics:
            plt.plot(financial_metrics['CashAndCashEquivalentsAtCarryingValue'].index,
                     financial_metrics['CashAndCashEquivalentsAtCarryingValue'], label='Cash & Cash Equivalents')
        if 'NetIncomeLoss' in financial_metrics:
            plt.plot(financial_metrics['NetIncomeLoss'].index,
                     financial_metrics['NetIncomeLoss'], label='Net Income/Loss', color='red')

        plt.title("SEC Financial Metrics")
        plt.xlabel('Date')
        plt.ylabel('USD Value')
        plt.legend()
        plt.grid()
        plt.savefig(output_path)
        plt.close()

        self.logger.info(f"Plot saved as '{output_path}'.")

# Example Usage
if __name__ == '__main__':
    from findatapy.util import SwimPool

    SwimPool()

    sec_vendor = DataVendorSEC()
    cik = "320193"  # Example CIK (Apple Inc.)

    financial_metrics = sec_vendor.load_financial_metrics(cik=cik)
    sec_vendor.save_plots(financial_metrics)

    print("Plot saved as 'financial_metrics.pdf'.")