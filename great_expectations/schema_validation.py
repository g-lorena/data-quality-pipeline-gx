import datetime
import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe
from generate_data import generate_product_data
from great_expectations.validator.validator import Validator

# Create Data Context.
context = gx.get_context()

# Create a pandas datasource.
data_source = context.sources.add_pandas(name="product_data_source")

# Read daframe
df = generate_product_data(scenario="extra_column")

# create data asset 
data_asset = data_source.add_dataframe_asset(name="product_dataframe")

# build batch request 
batch_request = data_asset.build_batch_request(dataframe=df)

suite1 = context.add_or_update_expectation_suite("product_inventory_schema_validation")
suite1 = context.add_or_update_expectation_suite("business_rules_validation")

# validate 
validator1 = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="product_inventory_schema_validation",
)
result = validator1.expect_table_column_count_to_equal(
    value=7
)

validator1.save_expectation_suite(discard_failed_expectations=False)


validator2 = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="product_inventory_schema_validation",
)

validator2 = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="business_rules_validation",
)

result = validator2.expect_column_values_to_match_regex(column="product_id", regex=r"^PROD_\d{3}$")

validator2.save_expectation_suite(discard_failed_expectations=False)

# create a checkpoint to run the validation
checkpoint = context.add_or_update_checkpoint(
    name="product_inventory_schema_validation_checkpoint",
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": "product_inventory_schema_validation",
        },
        {
            "batch_request": batch_request,
            "expectation_suite_name": "business_rules_validation",
        }
    ]
   # validator=validator
)

# execute the checkpoint
checkpoint_result = checkpoint.run()


 
if __name__ == "__main__":
    if checkpoint_result["success"]:
        print("PASS: All validations passed!")
    else:
        print("FAIL: Some validations failed")

         
        for validation_result in checkpoint_result.run_results.values():
            print("----------")
            print(validation_result)
            for expectation_result in validation_result["validation_result"]["results"]:
                if not expectation_result["success"]:
                    print(f"\nFailed expectation: {expectation_result['expectation_config']['expectation_type']}")
                    print(f"Details: {expectation_result.get('result', {})}")
        
        #print(f"FAIL: {result['unexpected_count']} rows have incorrect type for product_id")
        #print("Example of unexpected values:", result["result"]["partial_unexpected_list"])
        
