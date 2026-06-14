from dagster import MultiPartitionsDefinition, StaticPartitionsDefinition, DailyPartitionsDefinition

boe_partitions = MultiPartitionsDefinition(
    {
        "country": StaticPartitionsDefinition(["es"]),
        "date": DailyPartitionsDefinition(start_date="2010-01-01"),
    }
)
