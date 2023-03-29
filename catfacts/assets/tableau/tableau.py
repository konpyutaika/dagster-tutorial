from dagster import asset, AssetIn

@asset(ins={"redshift_table":AssetIn(key="redshift_table")})
def refresh_tableau(redshift_table):
    print(f'refresh after {redshift_table} received')
