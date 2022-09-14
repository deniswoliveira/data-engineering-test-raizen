# data-engineering-test-raizen
Case to Data Engineer on Raizen Analytics

# Objective
Extract internal pivot caches from consolidated reports made available by Brazilian government's regulatory agency for oil/fuels, ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis).

The developed pipeline is meant to extract and structure the underlying data of two of these tables:

Sales of oil derivative fuels by UF and product
Sales of diesel by UF and type
The totals of the extracted data must be equal to the totals of the pivot tables.

# Solution
## Python container deploy

`git clone https://github.com/deniswoliveira/data-engineering-test-raizen.git`

`cd data-engineering-test-raizen`

`docker build --pull --rm -f "Dockerfile" -t dataengineeringtestraizen:latest . `

`docker run --rm -it  dataengineeringtestraizen:latest`

###### Outuput
/opt/app/tests
contains test file -> xunit-reports

/opt/trusted_data
contains normalized files -> sales_diesel.parquet | sales_oil_derivative_fuels.parquet

###### Next steps

[ ] Add documentation

## Airflow deploy

`git clone https://github.com/deniswoliveira/data-engineering-test-raizen.git`

`cd data-engineering-test-raizen/airflow`

 `docker-compose up airflow-init`
 
 `docker-compose up -d`
 
 ###### Outuput
 airflow-worker
 
 /home/airflow/trusted/sales_oil_derivative_fuels.parquet
 
 /home/airflow/trusted/sales_diesel.parquet

###### Next steps
[ ] Modularize the code

[ ] Update code to use variables

[ ] Implement tests

[ ] Add documentation

