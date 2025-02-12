import pandas as pd

from .parser import ParadataSessions

class ParadataFile:

    def __init__(self, input_filename, output_filename, sep=None):
        if sep:
            self.data = pd.read_csv(input_filename, sep=sep)
        else:
            self.data = pd.read_csv(input_filename, on_bad_lines="warn")
        
        self.parser = ParadataSessions(self.data)


    def to_csv(self):
        self.session_sum_time_device()
        print(self.output)
        print(self.output['device_duration_1'])
        print(self.output['device_duration_1_seconds'])
        self.output.dropna(how='all', axis=1, inplace=True)
        self.output.fillna('.')
        self.output.to_csv(self.filename)


