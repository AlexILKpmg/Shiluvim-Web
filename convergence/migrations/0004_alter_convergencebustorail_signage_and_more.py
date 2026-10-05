from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("convergence", "0003_convergencebustorail_is_bus_on_time_and_more"),
    ]

    operations = [
        # Remove constraints before altering their columns.
        migrations.RemoveConstraint(
            model_name="convergencebustorail",
            name="uniq_cov_b2r_row",
        ),
        migrations.RemoveConstraint(
            model_name="convergencerailtobus",
            name="uniq_cov_r2b_row",
        ),

        # Keep the original field changes.
        migrations.AlterField(
            model_name="convergencebustorail",
            name="signage",
            field=models.IntegerField(),
        ),
        migrations.AlterField(
            model_name="convergencebustorail",
            name="train_number",
            field=models.IntegerField(),
        ),
        migrations.AlterField(
            model_name="convergencerailtobus",
            name="signage",
            field=models.IntegerField(),
        ),
        migrations.AlterField(
            model_name="convergencerailtobus",
            name="train_number",
            field=models.IntegerField(),
        ),

        # Restore uniqueness after altering the columns.
        migrations.AddConstraint(
            model_name="convergencebustorail",
            constraint=models.UniqueConstraint(
                fields=(
                    "year",
                    "month",
                    "week_period",
                    "train_station_name",
                    "rail_direction",
                    "train_number",
                    "makat",
                    "departure_time",
                ),
                name="uniq_cov_b2r_row",
            ),
        ),
        migrations.AddConstraint(
            model_name="convergencerailtobus",
            constraint=models.UniqueConstraint(
                fields=(
                    "year",
                    "month",
                    "week_period",
                    "train_station_name",
                    "rail_direction",
                    "train_number",
                    "makat",
                    "departure_time",
                ),
                name="uniq_cov_r2b_row",
            ),
        ),
    ]