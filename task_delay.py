task_1 = ...

delay_task_2 = DateTimeSensor(
    task_id="delay_task_2",
    target_time="{{ data_interval_end.replace(hour=15, minute=0, second=0, microsecond=0) }}", # Would not run till 9 am PST
    mode="reschedule",
    poke_interval=60,
    timeout=14400,
)

task_2 = ...

task_1 >> delay_task_2 >> task2
