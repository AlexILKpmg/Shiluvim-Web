from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("convergence", "0017_alter_rawbusdata_year"),
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

        migrations.AddField(
            model_name="rawbusdata",
            name="train_station_name",
            field=models.CharField(default="", max_length=255),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name="convergencebustorail",
            name="year",
            field=models.CharField(max_length=50),
        ),
        migrations.AlterField(
            model_name="convergencerailtobus",
            name="year",
            field=models.CharField(max_length=50),
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