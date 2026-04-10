import pandas as pd
import glob

# ==========================================
# PART 1: SOLD DATA
# ==========================================
# 1. Grab and Combine
sold_files = glob.glob("CRMLSSold*.csv")
all_sold = pd.concat([pd.read_csv(f) for f in sold_files])

# 2. Filter for Residential
res_sold = all_sold[all_sold['PropertyType'] == 'Residential']

# 3. Document (The Week 1 Requirement)
print(f"Sold Before Filter: {len(all_sold)}")
print(f"Sold After Residential Filter: {len(res_sold)}")

# 4. Save
res_sold.to_csv('combined_sold_residential.csv', index=False)


# ==========================================
# PART 2: LISTING DATA
# ==========================================
# 1. Grab and Combine
list_files = glob.glob("CRMLSListing*.csv")
all_listings = pd.concat([pd.read_csv(f) for f in list_files])

# 2. Filter for Residential
res_listings = all_listings[all_listings['PropertyType'] == 'Residential']

# 3. Document (The Week 1 Requirement)
print(f"Listings Before Filter: {len(all_listings)}")
print(f"Listings After Residential Filter: {len(res_listings)}")

# 4. Save
res_listings.to_csv('combined_listings_residential.csv', index=False)

print("✅ Week 1 Aggregation Complete!")