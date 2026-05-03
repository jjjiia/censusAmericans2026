import pandas as pd
import re

# ---- CONFIG ----
INPUT_FILE = "posts_04062026.csv"
OUTPUT_FILE = "posts_04062026_stateCap.csv"

# Set to None to apply to ALL columns, or list specific columns like ["state", "location"]
COLUMNS_TO_FIX = None  

# ---- US STATES LIST ----
states = [
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
    "Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan",
    "Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada",
    "New Hampshire","New Jersey","New Mexico","New York","North Carolina",
    "North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
    "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
    "Virginia","Washington","West Virginia","Wisconsin","Wyoming"
]

# Create lookup dict: lowercase → proper case
state_lookup = {s.lower(): s for s in states}

# Regex pattern to match any state name (case-insensitive)
pattern = re.compile(r'\b(' + '|'.join(re.escape(s.lower()) for s in states) + r')\b', re.IGNORECASE)

# ---- LOAD CSV ----
df = pd.read_csv(INPUT_FILE, dtype=str)

# ---- FUNCTION TO FIX STATES ----
def fix_states(text):
    if pd.isna(text):
        return text
    
    def replacer(match):
        return state_lookup[match.group(0).lower()]
    
    return pattern.sub(replacer, text)

# ---- APPLY ----
if COLUMNS_TO_FIX is None:
    columns = df.columns
else:
    columns = COLUMNS_TO_FIX

for col in columns:
    df[col] = df[col].apply(fix_states)

# ---- SAVE ----
df.to_csv(OUTPUT_FILE, index=False)

print(f"Done! Saved to {OUTPUT_FILE}")