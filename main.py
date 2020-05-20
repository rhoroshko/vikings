from vikings.vikings import Vikings
import pandas as pd
pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.expand_frame_repr = False
pd.options.mode.chained_assignment = None

vk = Vikings("", "en")

# print(vk.get_materials())
# print(vk.get_set("Грива Слейпнира"))
print(vk.get_set("Sleipnir's Mane"))
