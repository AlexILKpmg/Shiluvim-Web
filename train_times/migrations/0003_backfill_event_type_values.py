from django.db import migrations


def forward_convert_event_types(apps, schema_editor):
    TrainTime = apps.get_model("train_times", "TrainTime")
    TrainTime.objects.filter(event_type="ARRIVAL").update(event_type="to_tlv")
    TrainTime.objects.filter(event_type="DEPARTURE").update(event_type="from_tlv")


def backward_convert_event_types(apps, schema_editor):
    TrainTime = apps.get_model("train_times", "TrainTime")
    TrainTime.objects.filter(event_type="to_tlv").update(event_type="ARRIVAL")
    TrainTime.objects.filter(event_type="from_tlv").update(event_type="DEPARTURE")


class Migration(migrations.Migration):

    dependencies = [
        ("train_times", "0002_alter_traintime_event_type"),
    ]

    operations = [
        migrations.RunPython(forward_convert_event_types, backward_convert_event_types),
    ]
