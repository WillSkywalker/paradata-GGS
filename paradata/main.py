import pandas as pd
import argparse

from .parser import ParadataSessions

class ParadataFile:

    def __init__(self, input_filename, output_filename, sep=None, mode='simple', tablet=True):
        if sep:
            self.data = pd.read_csv(input_filename, sep=sep)
        else:
            self.data = pd.read_csv(input_filename, on_bad_lines="warn")
        
        self.filename = output_filename
        self.parser = ParadataSessions(self.data, mode, tablet)


    def to_csv(self):
        self.session_sum_time_device()
        print(self.output)
        print(self.output['device_duration_1'])
        print(self.output['device_duration_1_seconds'])
        self.output.dropna(how='all', axis=1, inplace=True)
        self.output.fillna('.')
        self.output.to_csv(self.filename)



def main():
    parser = argparse.ArgumentParser(description='Process paradata files.')
    parser.add_argument('input_filename', type=str, help='The input CSV file')
    parser.add_argument('output_filename', type=str, help='The output CSV file')
    parser.add_argument('-s' ,'--sep', type=str, default=None, help='The separator used in the CSV file')
    parser.add_argument('-m', '--mode', type=str, choices=['simple', 'switches'], default='simple', 
                        help='''
Mode for extracting user devices and durations. You can choose between 'simple' and 'switches':
 - 'simple': The simple mode will only give you the first and last device and total duration for each response ID.
 - 'switches': The switches mode will contain all devices used, as well as duration on each device.
''')
    parser.add_argument('-t' ,'--tablet', action=argparse.BooleanOptionalAction,
                        help='Treat tablets as an independent category')


    args = parser.parse_args()

    paradata_file = ParadataFile(args.input_filename, args.output_filename, args.sep, args.mode, args.tablet)
    paradata_file.to_csv()

if __name__ == "__main__":
    main()