# import json
# import pandas as pd

# from agents.profiler import profile_data
# from agents.rule_generator import generate_rules
# from agents.validator import validate_data
# from agents.healer import self_heal

# # Load dataset
# df = pd.read_csv("data/raw/olist_customers_dataset.csv")

# # Step 1: Profiling
# profile = profile_data(df)

# print("\nDATA PROFILE:\n")
# print(json.dumps(profile, indent=4))

# # Save profile report
# with open("outputs/bronze/profile_report.json", "w") as f:
#     json.dump(profile, f, indent=4)

# # Step 2: Rule Generation
# rules = generate_rules(profile)

# print("\nAI GENERATED RULES:\n")
# print(rules)

# # Step 3: Validation
# errors = validate_data(df)

# print("\nVALIDATION ERRORS:\n")
# print(json.dumps(errors, indent=4))

# # Step 4: Self Healing
# clean_df = self_heal(df)

# # Save cleaned dataset
# clean_df.to_csv(
#     "outputs/silver/clean_customers.csv",
#     index=False
# )

# print("\nSELF-HEALING COMPLETED")

# print("\nCLEAN DATA SAMPLE:\n")
# print(clean_df.head())

import pandas as pd

from workflow import app

# Load dataset
df = pd.read_csv("data/raw/olist_customers_dataset.csv")

# Initial state
initial_state = {
    "df": df,
    "profile": {},
    "rules": "",
    "errors": []
}

# Run workflow
result = app.invoke(initial_state)

print("\nFINAL WORKFLOW COMPLETED\n")

print(result)