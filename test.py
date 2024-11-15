from datetime import datetime, timedelta

today = datetime.now() + timedelta(days=3)
# today.day

print(today.day, today.weekday())