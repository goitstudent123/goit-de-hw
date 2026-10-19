# Homework 03: Spark DataFrame API

Load users, purchases and products from CSV, drop rows with missing values, and calculate purchase totals by product category: overall, for buyers aged 18-25, their share of 18-25 spending, and the top 3 categories by that share.

Purchase amount = `quantity * price`.

## How to run

Requires [uv](https://docs.astral.sh/uv/) and Java 17 or 21 on `PATH` (Spark 4.1 doesn't run on Java 25).

`./run.sh` sets up the venv, runs the tests, then `main.py`. Input files are in `data/`.

## Results

Full run output: [results/output.txt](results/output.txt), tests: [results/tests.txt](results/tests.txt).

Screenshots of the `df.show()` output for each step: [1](results/1_raw_data.png), [2](results/2_cleaning.png), [3](results/3_total_by_category.png), [4](results/4_total_18_25.png), [5](results/5_share_18_25.png), [6](results/6_top_3.png).

1. Loaded: users 100, purchases 200, products 50 rows.
2. After `dropna()`: users 95, purchases 195, products 47.

3. Total by category:

| category    | total  |
|-------------|--------|
| Beauty      | 459.9  |
| Clothing    | 790.3  |
| Electronics | 1174.8 |
| Home        | 1523.5 |
| Sports      | 1802.5 |

4-5. Age 18-25, total and share:

| category    | total | percentage |
|-------------|-------|------------|
| Beauty      | 41.4  | 3.43       |
| Clothing    | 245.0 | 20.29      |
| Electronics | 249.6 | 20.67      |
| Home        | 361.1 | 29.9       |
| Sports      | 310.5 | 25.71      |

6. Top 3 categories, age 18-25: Home (29.9%), Sports (25.71%), Electronics (20.67%).
