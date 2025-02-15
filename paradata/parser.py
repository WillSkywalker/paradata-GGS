# Total response time ua related functions

import datetime
import pandas as pd
import numpy as np

from collections import Counter
from user_agents import parse


import xml.etree.ElementTree as ET
from collections import Counter

class NoOSStringError(Exception):
    pass



class ParadataSessions:
    
    def __init__(self, df, mode, tablet):
        self.dataframe = df
        self.mode = mode
        self.tablet = tablet

        self.integrate_switchsessions()

        sessions = self.dataframe[self.dataframe['Content'].str.startswith('<StartSessionEvent') & self.dataframe['respid']]
        max_session_num = max(Counter(sessions['respid']).values())
        columns_names = ['respid', 'num_switches', 'num_sessions', 'total_duration', 'total_duration_seconds', 'first_device', 'last_device']
        if mode == 'switches':
            indices = ['device_duration_' + str(i) for i in range(1, max_session_num+1)]
            indices = ['device_duration_' + str(i) + "_seconds" for i in range(1, max_session_num+1)]
            indices += ['switch_' + str(i) for i in range(1, max_session_num)]
            indices += ['session_' + str(i) for i in range(1, max_session_num+1)]
            indices += ['session_' + str(i) + '_seconds' for i in range(1, max_session_num+1)]
            columns_names += indices
        self.output = pd.DataFrame(columns=columns_names)
        
        
        self.dataframe['time'] = self.dataframe['TimeStamp'].apply(self.to_timestamp)
        self.dataframe['time'].loc[self.dataframe['Content'].str.startswith('<StartSessionEvent ')] -= datetime.timedelta(seconds=1)
        self.dataframe = self.dataframe.sort_values('time')
        self.dataframe['respid'].replace('', np.nan, inplace=True)
        self.dataframe.dropna(subset=['respid'], inplace=True)

        self.groups = self.dataframe.groupby('respid', as_index=False)
        print('Groups: ', str(len(self.groups)))
        
    def integrate_switchsessions(self):
        switches = self.dataframe[self.dataframe['Content'].str.startswith('<SwitchSessionEvent')]
        count = 0
        for switch in switches.iloc:
            old_id = '{' + switch['Content'].split('"')[1] + '}'
            print(old_id)
#             print(self.dataframe.loc[(self.dataframe['v1'] == switch['v1']) & (~self.dataframe['v4'].str.startswith('<SwitchSessionEvent')), 'respid'])
            respid = self.dataframe[self.dataframe['0'] == old_id]['respid'].iloc[0]
            self.dataframe.loc[(self.dataframe['0'] == switch['0']) & (~self.dataframe['Content'].str.startswith('<SwitchSessionEvent')), 'respid'] = respid
            count += 1
        print('%d switch sessions added.' % count)

    @staticmethod
    def to_timestamp(s):
        if s.split()[0].split('-')[0] == '18':
            return datetime.datetime.strptime(s, '%d-%m-%Y %H:%M')
        elif len(s.split()[1].split('.')) == 1:
            return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        else:
            return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')

    def get_device(self, event):
        root = ET.fromstring(event)
        try:
            ua = parse(root.attrib['OS'])
        except Exception as e:
            raise NoOSStringError()
        family = '' # ua.os.family
        if ua.is_tablet:
            family += 'Tablet' if self.tablet else 'Mobile'
        elif ua.is_mobile and not family.endswith('Phone'):
            family += 'Phone' if self.tablet else 'Mobile'
        elif ua.is_pc:
            family += 'PC'
        return family


    def session_time_device_per_respid(self, df):

        last_start_sessions = {}
        startsession_indices = df[df['Content'].str.startswith('<StartSessionEvent')].index
        endsession_indices = startsession_indices - 1
        endsession_indices = endsession_indices[1:]
        endsession_indices = endsession_indices.union([len(df) - 1])
        total_time = datetime.timedelta()
        
        if startsession_indices.empty:
            return

        num_session = 1
        num_logins = 0
        num_switches = 0
        
        last_device = self.get_device(df.loc[startsession_indices[0]]['Content'])
        respid = df.loc[0]['respid']
        self.output.loc[self.output.shape[0]] = {'respid': respid, 'first_device': last_device}
        delta = datetime.timedelta()


        for s, e in zip(startsession_indices, endsession_indices):
            row = df.loc[s]
            try:
                device = self.get_device(row['Content'])
            except NoOSStringError:
                print('error on respid: ', respid)
                print(row)
                print('=======\n')
                continue

            num_logins += 1   
            session_delta = df.loc[e]['time'] - df.loc[s]['time']
            total_time += session_delta
            sess_colname = 'session_' + str(num_logins)
            if self.mode == 'switches':
                self.output.loc[self.output['respid'] == respid, sess_colname] = str(session_delta)
                self.output.loc[self.output['respid'] == respid, sess_colname + '_seconds'] = session_delta.total_seconds()
            
            if device != last_device:

                colname = 'device_duration_' + str(num_session)

                num_switches += 1
                colname_switch = 'switch_' + str(num_switches)                    

                if self.mode == 'switches':
                    self.output.loc[self.output['respid'] == respid, colname] = str(delta)
                    self.output.loc[self.output['respid'] == respid, colname + '_seconds'] = delta.total_seconds()
                    self.output.loc[self.output['respid'] == respid, colname_switch] = last_device + ' to ' + device

                delta = df.loc[e]['time'] - df.loc[s]['time'] - datetime.timedelta(seconds=1)
                num_session += 1
            
            else:
                delta += df.loc[e]['time'] - df.loc[s]['time'] - datetime.timedelta(seconds=1)

            last_device = device

        if self.mode == 'switches':
            colname = 'device_duration_' + str(num_session)
            self.output.loc[self.output['respid'] == respid, colname] = str(delta)
            self.output.loc[self.output['respid'] == respid, colname + '_seconds'] = delta.total_seconds()

        self.output.loc[self.output['respid'] == respid, 'num_switches'] = num_switches
        self.output.loc[self.output['respid'] == respid, 'num_sessions'] = num_logins
        self.output.loc[self.output['respid'] == respid, 'total_duration'] = str(total_time)
        self.output.loc[self.output['respid'] == respid, 'total_duration_seconds'] = total_time.total_seconds()
        self.output.loc[self.output['respid'] == respid, 'last_device'] = last_device


    def session_sum_time_device(self):
        i = 0
        for name, group in self.groups:
            self.session_time_device_per_respid(group.reset_index())
            i += 1
        print(str(i), ' loops executed')
            



    
