from copy import deepcopy
from datetime import datetime

from ...external import array, arange, compare, diff, fill_array, float64, logical_and, ones, nan, numpy_found

class UpperAirSounding(object):
    __slots__ = ('__ascent_only__', '__mask__', 'station_id', 'station_latitude', 'station_longitude', 'station_elevation', 
                 'sounding_datetime', 'pressure', 'height', 'dry_buld_temperature', 'dewpoint_temperature', 'wind_direction', 
                 'wind_speed')
    def __init__(self, bufr_file, ascent_only=False):
        self.__ascent_only__ = ascent_only
        self.__mask__ = None

        self.station_id = None
        self.station_latitude = None
        self.station_longitude = None
        self.station_elevation = None
        self.sounding_datetime = None
        
        self.pressure = None
        self.height = None
        self.dry_buld_temperature = None
        self.dewpoint_temperature = None
        self.wind_direction = None
        self.wind_speed = None

        content = sorted(bufr_file.data.to_dict(filter_keys=['SMID', 'YEAR', 'MNTH', 'DAYS', 'HOUR', 'MINU', 'SECO', 'CLATH', 'CLONH', 'HSMSL', 'LTDS', 'PRLC', 'GPH10', 'LATDH', 'LONDH', 'TMDB', 'TMDP', 'WDIR', 'WSPD']), key=lambda a: a['LTDS'] if a['LTDS'] is not None else 999999999999999)

        if content[0]['SMID'] != '':
            self.station_id = content[0]['SMID']
        
        self.station_latitude = content[0]['CLATH']
        self.station_longitude = content[0]['CLONH']
        self.station_elevation = content[0]['HSMSL']
        self.sounding_datetime = datetime(content[0]['YEAR'], content[0]['MNTH'], content[0]['DAYS'], content[0]['HOUR'], content[0]['MINU'], content[0]['SECO'])

        content_length = len(content)

        for i in range(content_length):
            if 'PRLC' in content[i]:
                if self.pressure is None:
                    self.pressure = fill_array(content_length, nan, dtype=float64)
            if 'GPH10' in content[i]:
                if self.height is None:
                    self.height = fill_array(content_length, nan, dtype=float64)
            if 'TMDB' in content[i]:
                if self.dry_buld_temperature is None:
                    self.dry_buld_temperature = fill_array(content_length, nan, dtype=float64)
            if 'TMDP' in content[i]:
                if self.dewpoint_temperature is None:
                    self.dewpoint_temperature = fill_array(content_length, nan, dtype=float64)
            if 'WDIR' in content[i]:
                if self.wind_direction is None:
                    self.wind_direction = fill_array(content_length, nan, dtype=float64)
            if 'WSPD' in content[i]:
                if self.wind_speed is None:
                    self.wind_speed = fill_array(content_length, nan, dtype=float64)

        for i in range(content_length):
            if 'PRLC' in content[i]:
                if content[i]['PRLC'] is not None:
                    self.pressure[i] = content[i]['PRLC']
            if 'GPH10' in content[i]:
                if content[i]['GPH10'] is not None:
                    self.height[i] = content[i]['GPH10']
            if 'TMDB' in content[i]:
                if content[i]['TMDB'] is not None:
                    self.dry_buld_temperature[i] = content[i]['TMDB']
            if 'TMDP' in content[i]:
                if content[i]['TMDP'] is not None:
                    self.dewpoint_temperature[i] = content[i]['TMDP']
            if 'WDIR' in content[i]:
                if content[i]['WDIR'] is not None:
                    self.wind_direction[i] = content[i]['WDIR']
            if 'WSPD' in content[i]:
                if content[i]['WSPD'] is not None:
                    self.wind_speed[i] = content[i]['WSPD']

        if self.__ascent_only__:
            if self.pressure is not None:
                self.pressure = self.__apply_mask__(self.pressure)
            if self.height is not None:
                self.height = self.__apply_mask__(self.height)
            if self.dry_buld_temperature is not None:
                self.dry_buld_temperature = self.__apply_mask__(self.dry_buld_temperature)
            if self.dewpoint_temperature is not None:
                self.dewpoint_temperature = self.__apply_mask__(self.dewpoint_temperature)
            if self.wind_direction is not None:
                self.wind_direction = self.__apply_mask__(self.wind_direction)
            if self.wind_speed is not None:
                self.wind_speed = self.__apply_mask__(self.wind_speed)

    def __apply_mask__(self, values):
        if self.__mask__ is None:
            self.__mask__ = self.__get_ascent_only_mask__()
        if numpy_found:
            new_values = values[self.__mask__]
        else:
            new_values = [values[i] for i in self.__mask__]
        return new_values

    def __get_ascent_only_mask__(self):
        index = arange(len(self.pressure))
        height = deepcopy(self.height)
        pressure = deepcopy(self.pressure)
        mask = logical_and(compare(diff(height, n=1, prepend=height[0]-1), '>', 0),
                            compare(diff(pressure, n=1, prepend=pressure[0]+1), '<', 0))
        while sum(mask) != len(mask):
            index = array([index[i] for i, x in enumerate(mask) if x])
            height = array([height[i] for i, x in enumerate(mask) if x])
            pressure = array([pressure[i] for i, x in enumerate(mask) if x])
            mask = logical_and(compare(diff(height, n=1, prepend=height[0]-1), '>', 0),
                                compare(diff(pressure, n=1, prepend=pressure[0]+1), '<', 0))
        return index