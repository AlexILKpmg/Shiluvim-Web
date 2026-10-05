from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("convergence", "0005_alter_convergencebustorail_direction_and_more"),
    ]

    operations = [
        migrations.RemoveConstraint(
            model_name="convergencebustorail",
            name="uniq_cov_b2r_row",
        ),
        migrations.RemoveConstraint(
            model_name="convergencerailtobus",
            name="uniq_cov_r2b_row",
        ),

        migrations.AlterField(
            model_name="convergencebustorail",
            name="makat",
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name="convergencerailtobus",
            name="makat",
            field=models.IntegerField(blank=True, null=True),
        ),

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