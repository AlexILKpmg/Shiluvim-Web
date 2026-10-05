from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("convergence", "0025_remove_overrideconv_train_station_code_and_more"),
    ]

    operations = [
        # The old constraint is absent from the local database.
        # Remove it only from Django's migration state.
        migrations.SeparateDatabaseAndState(
            database_operations=[],
            state_operations=[
                migrations.RemoveConstraint(
                    model_name="overrideconv",
                    name="uniq_override_conv_default_key",
                ),
            ],
        ),

        # Create the new constraint in both the database and Django's state.
        migrations.AddConstraint(
            model_name="overrideconv",
            constraint=models.UniqueConstraint(
                fields=(
                    "week_period",
                    "link_direction",
                    "makat",
                    "direction",
                    "alternative",
                    "departure_time",
                    "station_name",
                    "from_train_number",
                    "from_train_rishui_train_arrival_time",
                ),
                name="uniq_override_conv_default_key",
            ),
        ),

        migrations.RemoveField(
            model_name="overrideconv",
            name="disable_reason",
        ),
        migrations.RemoveField(
            model_name="overrideconv",
            name="disabled_at",
        ),
        migrations.RemoveField(
            model_name="overrideconv",
            name="disabled_by",
        ),
        migrations.RemoveField(
            model_name="overrideconv",
            name="is_enabled",
        ),
    ]