import pandas as pd
import glob
import sys

# --- PHASE 1: LOAD & FILTER ---
print("1. Loading Sold Data...")
sold_files = glob.glob("CRMLSSold*.csv")
if not sold_files:
    print("Error: No Sold files found! Check your folder."); sys.exit()

df_sold = pd.concat([pd.read_csv(f, low_memory=False) for f in sold_files])
print(f"   Property Types Found: {df_sold['PropertyType'].unique()}")
df_sold = df_sold[df_sold['PropertyType'] == 'Residential'].copy()

# --- PHASE 2: MORTGAGE DATA ---
print("\n2. Fetching Mortgage Data...")
url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=MORTGAGE30US"
try:
    mortgage = pd.read_csv(url)
    mortgage.columns = ['DATE', 'rate_30yr_fixed']
    mortgage['DATE'] = pd.to_datetime(mortgage['DATE'])
    mortgage['year_month'] = mortgage['DATE'].dt.to_period('M')
    mortgage_monthly = mortgage.groupby('year_month')['rate_30yr_fixed'].mean().reset_index()
    print("   Mortgage data ready.")
except Exception as e:
    print(f"   Mortgage Download Failed: {e}"); sys.exit()

# --- PHASE 3: DATE CONVERSION & MERGE ---
print("\n3. Processing Dates and Merging...")

# Speed Trick: cache=True makes 381k rows process in seconds
df_sold['CloseDate'] = pd.to_datetime(df_sold['CloseDate'], errors='coerce', cache=True)

# Remove rows without a date
df_sold = df_sold.dropna(subset=['CloseDate'])
df_sold['year_month'] = df_sold['CloseDate'].dt.to_period('M')

print("   Merging Sold data...")
final_sold = df_sold.merge(mortgage_monthly, on='year_month', how='left')
final_sold.to_csv('final_enriched_sold_data.csv', index=False)
print("   Sold Enriched and saved.")

# Listings Merge
listing_files = glob.glob("CRMLSListing*.csv")
if listing_files:
    df_list = pd.concat([pd.read_csv(f, low_memory=False) for f in listing_files])
    df_list = df_list[df_list['PropertyType'] == 'Residential'].copy()
    
    # Check for date column in listings
    list_date_col = 'ListingContractDate' if 'ListingContractDate' in df_list.columns else df_list.columns[0]
    df_list['year_month'] = pd.to_datetime(df_list[list_date_col], errors='coerce', cache=True).dt.to_period('M')
    
    final_listed = df_list.merge(mortgage_monthly, on='year_month', how='left')
    final_listed.to_csv('final_enriched_listed_data.csv', index=False)
    print("   Listings Enriched and saved.")

# --- PHASE 4: SUMMARY FOR SUBMISSION ---
print("\n--- DISTRIBUTION SUMMARY ---")
cols_to_check = ['ClosePrice', 'LivingArea', 'DaysOnMarket']
existing_cols = [c for c in cols_to_check if c in df_sold.columns]
print(df_sold[existing_cols].describe())

# Check for nulls after merge
null_check = final_sold['rate_30yr_fixed'].isnull().sum()
print(f"\n✅ ALL DONE. Null Mortgage Rates: {null_check}")