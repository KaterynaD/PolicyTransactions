Welcome to your PolicyTransactions dbt project!

### Using the project:

dbt seed

dbt run --select tag:dim  --vars '{"new_transactiondate":"20200131","load_defaults":True}'

dbt test --select dim_vehicle dim_driver dim_transaction --exclude fact_policytransaction

dbt run --select fact_policytransaction  --vars '{"new_transactiondate":"20200131"}'

dbt test --vars '{"new_transactiondate":"20200131"}'

dbt run --vars '{"loaddate":"2024-11-28","latest_loaded_transactiondate":"20200131","new_transactiondate":"20200331","load_defaults":False}'

dbt test --vars '{"loaddate":"2024-11-28","latest_loaded_transactiondate":"20200131","new_transactiondate":"20200331"}'


dbt run --vars '{"loaddate":"2024-11-29","latest_loaded_transactiondate":"20200331","new_transactiondate":"20201231","load_defaults":False}'

dbt test --vars '{"loaddate":"2024-11-29","latest_loaded_transactiondate":"20200331","new_transactiondate":"20201231"}'


