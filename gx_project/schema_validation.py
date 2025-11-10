import datetime
import os
import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.validator.validator import Validator
from scripts.generate_dirty_data import generate_dirty_inventory_data
from great_expectations.checkpoint.actions import SlackNotificationAction

# Create Data Context.
context = gx.get_context()

data_source_name = "product_data_source"

# Create a pandas datasource.
#data_source = context.sources.add_pandas(name="product_data_source")
data_source = context.data_sources.add_pandas(name=data_source_name)

# Read daframe
df = generate_dirty_inventory_data(n_rows=100, scenario="mixed")

# create data asset 
data_asset = data_source.add_dataframe_asset(name="product_dataframe")

batch_definition_name = "my_batch_definition"

batch_definition = data_asset.add_batch_definition_whole_dataframe(
    name=batch_definition_name
)

suite1 = context.suites.add(
    gx.ExpectationSuite(name="product_inventory_schema_validation")
)

suite2 = context.suites.add(
    gx.ExpectationSuite(name="business_rules_validation")
)

suite1.add_expectation(
    gxe.ExpectTableColumnCountToEqual(value=7)
)

suite2.add_expectation(
    gxe.ExpectColumnValuesToMatchRegex(column="product_id", regex=r"^PROD_\d{3}$")
)


validation_definition1 = gx.ValidationDefinition(
    data=batch_definition, suite=suite1, name="product_inventory_schema_validation"
)

validation_definition1 = context.validation_definitions.add(validation_definition1)

validation_definition2 = gx.ValidationDefinition(
    data=batch_definition, suite=suite2, name="business_rules_validation"
)
validation_definition2 = context.validation_definitions.add(validation_definition2)

slack_webhook = os.getenv("SLACK_WEBHOOK_URL")

actions = []
if slack_webhook:
    slack_action = SlackNotificationAction(
        name="slack_notification",
        slack_webhook=slack_webhook,  # Depuis variable d'environnement
        notify_on="failure",
        renderer={
            "module_name": "great_expectations.render.renderer.slack_renderer",
            "class_name": "SlackRenderer",
        },
    )
    actions.append(slack_action)
    print("✅ Slack notifications enabled")
else:
    print("⚠️  SLACK_WEBHOOK_URL not set. Slack notifications disabled.")


checkpoint = gx.Checkpoint(
    name="product_inventory_checkpoint",
    validation_definitions=[validation_definition1, validation_definition2],
    actions=actions,
    result_format={"result_format": "COMPLETE"},
)

checkpoint = context.checkpoints.add(checkpoint)

checkpoint_result = checkpoint.run(
    batch_parameters={"dataframe": df}  
)
 
if __name__ == "__main__":
    if checkpoint_result.success:
        print("PASS: All validations passed!")
    else:
        print("FAIL: Some validations failed")

        for validation_result in checkpoint_result.run_results.values():
            # Correction ici: pas besoin de ["validation_result"]
            print(f"\nSuite: {validation_result.suite_name}")
            
            for result in validation_result.results:
                if not result.success:
                    print(f"  ✗ {result.expectation_config.type}")
                    print(f"    {result.result}")
        
        print()
        
