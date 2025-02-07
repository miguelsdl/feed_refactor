import json

import datetime
# a = "'available_128',true,'available_320',true,'available_preview',true".split(',')
# # b = {a[i].replace("'", ""): a[i + 1].replace("'", "") for i in range(0, len(a), 2)}
#
# specific_data_track = {}
# for i in range(0, len(a), 2):
#     key = a[i].replace("'", "")
#     val = a[i + 1].replace("'", "")
#     specific_data_track[key] = True if val == "true" else val
#
# print(json.dumps(specific_data_track))


time_str = str(datetime.timedelta(seconds=143))
print("Method 1 (timedelta):")
print(time_str)