import sys
sys.path.append("/Users/danramage/Documents/workspace/CDMO/python/common")
#sys.path.append("D:\scripts\common")
from os.path import join

import logging.config
import optparse
#import requests
import csv
import ConfigParser
from datetime import datetime
from pytz import timezone

import shelve
from mako.template import Template
from mako import exceptions as makoExceptions
import simplejson

from smtp_utils import smtpClass

class station_metadata(object):
  def __init__(self, use_logging=True, **kwargs):

    self.logger = None
    if use_logging:
      self.logger = logging.getLogger(__name__)

    self.reserve_code = kwargs.get('reserve_code', None)
    self.station_code = kwargs.get('station_code', None)
    self.station_name = kwargs.get('station_name', None)
    self.longitude = kwargs.get('longitude', None)
    self.latitude = kwargs.get('latitude', None)
    self.active_dates = kwargs.get('active_dates', None)
    self.state = kwargs.get('state', None)
    self.hads_id = kwargs.get('hads_id', None)
    if len(self.hads_id) < 8:
      self.hads_id = None
    self.goes_satellite = kwargs.get('goes_satellite', None)
    self.station_type = kwargs.get('station_type', None)
    self.region = kwargs.get('region', None)
    self.is_swmp = kwargs.get('is_swmp', None)
    self.reserve_name = kwargs.get('reserve_name', None)

    self.transmit_channel = kwargs.get('transmit_channel', None)
    self.transmit_time = kwargs.get('transmit_time', None)       #Time the station transmits
    self.export_time = kwargs.get('export_time', None)         #Time the decoding and exporting process writes the csv output file.
                                    #Currently we have exports at 00:12:00 and 00:42:00 each hour.
    self.min_recs_per_hour = kwargs.get('min_recs_per_hour', 4)      #Number of records a station should produce each hour.

    if self.transmit_time is not None:
      self.transmit_time = datetime.strptime(self.transmit_time, "%H:%M:%S")
    if self.export_time is not None:
      self.export_time = datetime.strptime(self.export_time, "%H:%M:%S")

  def __getstate__(self):
    d = dict(self.__dict__)
    # Cannot pickle the logger object, but if we are using the logger we'll delete the object
    # but add the name of the object used to create the logger.
    if self.logger is not None:
      del d['logger']
      d['logger'] = __name__
    return d

  def __setstate__(self, state):
    self.__dict__ = state
    self.logger = None
    if 'logger' in state:
      self.logger = logging.getLogger(state['logger'])
    if state['hads_id'] is not None and len(state['hads_id']) < 8:
      self.hads_id = None

  def to_dict(self):
    transmit_time = None
    if self.transmit_time is not None:
      transmit_time = self.transmit_time.strftime('%H:%M:%S')
    export_time = None
    if self.export_time is not None:
      export_time = self.export_time.strftime('%H:%M:%S')

    ret_val = {
      'reserve_code': self.reserve_code,
      'station_code': self.station_code,
      'station_name': self.station_name,
      'longitude': self.longitude,
      'latitude': self.latitude,
      'active_dates': self.active_dates,
      'state': self.state,
      'hads_id': self.hads_id,
      'station_type': self.station_type,
      'region': self.region,
      'is_swmp': self.is_swmp,
      'reserve_name': self.reserve_name,
      'transmit_channel': self.transmit_channel,
      'transmit_time': transmit_time,
      'export_time': export_time,
      'min_recs_per_hour': self.min_recs_per_hour,
      'goes_satellite': self.goes_satellite
    }
    return ret_val

class station_status(object):
  (ALL_DATA, OLD_DATA, MISSING_RECS, FUTURE_DATA, MISSING_FILE, NO_DATA) = (0, 1, 2, 3, 4, 5)
  def __init__(self, use_logging=True, **kwargs):
    self.logger = None
    if use_logging:
      self.logger = logging.getLogger(__name__)

    self.station_code = kwargs.get('station_code', None)            #CDMO station code
    self.last_check_status_time = kwargs.get('last_check_status_time', None)  #The last time the status check was performed for the station
    self.last_update_time = kwargs.get('last_update_time', None)        #Last update time in the data
    self.allowed_hour_count_to_miss = kwargs.get('allowed_hour_count_to_miss', 4) #Number of hourly tranmissions station can miss before reporting a miss.
    self.current_hour_count_missed = kwargs.get('current_hour_count_missed', 0)  #How many transmissions the station has currently missed.

    self.disable_test = kwargs.get('disable_test', False)
    self.status_field = kwargs.get('status_field', None)
    self.record_updated = kwargs.get('record_updated', False)

    self.current_telemetry_dates_cnt = 0

    if self.last_update_time is not None:
      self.last_update_time = datetime.strptime(self.last_update_time, "%Y-%m-%d %H:%M:%S")
    if self.last_check_status_time is not None:
      self.last_check_status_time = datetime.strptime(self.last_check_status_time, "%Y-%m-%d %H:%M:%S")

  def __getstate__(self):
    d = dict(self.__dict__)
    # Cannot pickle the logger object, but if we are using the logger we'll delete the object
    # but add the name of the object used to create the logger.
    if self.logger is not None:
      del d['logger']
      d['logger'] = __name__

    del d['current_telemetry_dates_cnt'] #This variable is only valid for a specific check_status.
    d['record_updated'] = False # We always want this False when pickling.
    return d

  def __setstate__(self, state):
    self.__dict__ = state
    self.logger = None
    if 'logger' in state:
      self.logger = logging.getLogger(state['logger'])
    self.current_telemetry_dates_cnt = 0

  def set_allowed_hours_to_miss(self, hours):
    if self.logger:
      self.logger.debug("set_allowed_hours_to_miss: %d" % (hours))
    self.allowed_hour_count_to_miss = hours
    self.record_updated = True

  def set_last_update_time(self, last_update_time):
    if self.logger:
      self.logger.debug("set_last_update_time: %d" % (last_update_time))
    self.last_update_time = last_update_time
    self.record_updated = True

  def set_disable_test(self, disable_flag):
    if self.logger:
      self.logger.debug("disable_test: %s" % (disable_flag))
    self.disable_test = disable_flag
    self.record_updated = True

  def is_test_disabled(self):
    return self.disable_test

  def get_allowed_hours_to_miss(self):
    return self.allowed_hour_count_to_miss

  def get_current_hour_count_missed(self):
    return self.current_hour_count_missed

  def get_last_update_time(self):
    return self.last_update_time

  def get_last_update_time_string(self, tz_obj=None):
    if tz_obj is None:
      return self.last_update_time.strftime("%Y-%m-%d %H:%M:%S")
    else:
      if self.last_update_time is not None:
        return (timezone('UTC').localize(self.last_update_time).astimezone(tz_obj)).strftime("%Y-%m-%d %H:%M:%S")

    return ""

  def check_status(self, **kwargs):
    check_fail = False
    self.status_field = None
    current_hour = kwargs['status_check_time']
    self.record_updated = True
    if not self.disable_test:
      self.status_field = []
      if self.logger:
        self.logger.debug("Station: %s checking status." % (self.station_code))
      current_telemetry_dates = self.get_current_telemetry_times(kwargs['telemetry_export_directory'])

      #Couldn't find a file.
      if current_telemetry_dates is None:
        self.status_field = [station_status.MISSING_FILE]
        check_fail = True
        self.current_hour_count_missed += 1

      else:

        #We have a file, but no records in it.
        if len(current_telemetry_dates) == 0:
          self.current_hour_count_missed += 1
          self.status_field = [station_status.NO_DATA]
          check_fail = True


        else:
          #We've got a file with records, but not all the records.
          self.current_telemetry_dates_cnt = len(current_telemetry_dates)
          if self.current_telemetry_dates_cnt < kwargs['min_recs_per_hour']:
            self.current_hour_count_missed += 1
            self.status_field = [station_status.MISSING_RECS]
            check_fail = True
          #Dates are stored in ascending manner, so let's start at back with our check.
          rec_cnt = 0
          #For now, we just check the last record in the file.
          self.last_update_time = current_telemetry_dates[-1]
          if current_hour > self.last_update_time:
            offset = current_hour - self.last_update_time
            if offset.seconds >= (2 * 3600):
            #if offset.seconds >= (self.allowed_hour_count_to_miss * 3600):
              self.status_field.append(station_status.OLD_DATA)
              check_fail = True
          else:
            offset = self.last_update_time - current_hour
            self.status_field.append(station_status.FUTURE_DATA)
            check_fail = True

          """
          for date_rec in reversed(current_telemetry_dates):
            if rec_cnt == self.min_recs_per_hour:
              break
            if current_hour > date_rec:
              offset = current_hour - date_rec
              if offset.seconds >= (self.allowed_hour_count_to_miss * 3600):
                self.curent_hour_count_missed += 1
                status.append(station_status.OLD_DATA)
                break
            else:
              offset = date_rec - current_hour
            rec_cnt += 1
            """
    #Did not fail the check, so reset count.
    if not check_fail:
      self.current_hour_count_missed = 0

    self.last_check_status_time = current_hour

    return check_fail

  def get_current_telemetry_times(self, telemetry_export_directory):
    #export_csv_file = '%s/%s.csv' % (telemetry_export_directory, self.station_code)
    export_csv_file = join(telemetry_export_directory, "%s.csv" % (self.station_code))
    dates_present = None
    try:
      with open(export_csv_file, "r") as data_export_file:
        dates_present = []
        csv_file = csv.reader(data_export_file, delimiter=',', quotechar='"')
        line_cnt = 0
        for row in csv_file:
          if line_cnt > 0:
            #For both wq and met csv files there is always a header line and each
            #data row, first column is date, format 09/10/2013, second is time, format 12:15:00.
            try:
              date_string = "%s %s" % (row[0], row[1])
              date_time_val = datetime.strptime(date_string, '%m/%d/%Y %H:%M:%S')
              dates_present.append(date_time_val)
            except (ValueError, Exception) as e:
              #Could be odd time format, so one more try
              try:
                date_time_val = datetime.strptime(date_string, '%m/%d/%Y %H:%M')
                dates_present.append(date_time_val)
              except ValueError,e:
                if self.logger:
                  self.logger.exception(e)

          line_cnt += 1
    except IOError,e:
      if self.logger:
        self.logger.exception(e)

    return(dates_present)

  def save_settings(self):
    return

  def to_dict(self, tz_obj=None):
    last_update_time = None
    if self.last_update_time is not None:
      #Natively we work in UTC, but if we pass in a tz_obj, we convert to the tz.
      if tz_obj is None:
        last_update_time = self.last_update_time.strftime("%Y-%m-%d %H:%M:%S")
      else:
        last_update_time = (timezone('UTC').localize(self.last_update_time).astimezone(tz_obj)).strftime("%Y-%m-%d %H:%M:%S")

    last_check_status_time = None
    if self.last_check_status_time is not None:
      if tz_obj is None:
        last_check_status_time = self.last_check_status_time.strftime("%Y-%m-%d %H:%M:%S")
      else:
        last_check_status_time = (timezone('UTC').localize(self.last_check_status_time).astimezone(tz_obj)).strftime("%Y-%m-%d %H:%M:%S")

    station_nfo = {
      'station_code': self.station_code,
      'last_update_time' : last_update_time,
      'last_check_status_time': last_check_status_time,
      'allowed_hour_count_to_miss': self.allowed_hour_count_to_miss,
      'current_hour_count_missed': self.current_hour_count_missed,
      'disable_test': self.disable_test,
      'status_field': self.status_field
    }
    return station_nfo

  def decode_current_status(self):
    message_list = []
    if self.status_field is not None:
      for status in self.status_field:
        if status == station_status.NO_DATA:
          message_list.append("No data present in file.")
        elif status == station_status.OLD_DATA:
          message_list.append("Old data present in file. Last record timestamp: %s" % (self.last_update_time.strftime("%Y-%m-%d %H:%M:%S")))
        elif status == station_status.FUTURE_DATA:
          message_list.append("Data in file is newer than testing time.")
        elif status == station_status.MISSING_FILE:
          message_list.append("No telemetry data file found.")
        elif status == station_status.MISSING_RECS:
          message_list.append("File is missing telemetry records.")
        elif status == station_status.ALL_DATA:
          message_list.append("Telemetry is good.")
    return message_list

class station_telemetry_stat(object):
  def __init__(self, use_logging=True):
    self.logger = None
    if use_logging:
      self.logger = logging.getLogger(__name__)
    self.station_code = None
    self.record_datetime = None
    self.record_count = 0

  def __getstate__(self):
    d = dict(self.__dict__)
    # Cannot pickle the logger object, but if we are using the logger we'll delete the object
    # but add the name of the object used to create the logger.
    if self.logger is not None:
      del d['logger']
      d['logger'] = __name__

    return d

  def __setstate__(self, state):
    self.__dict__ = state
    self.logger = None
    if 'logger' in state:
      self.logger = logging.getLogger(state['logger'])

  def set_statistic(self, station_code, date_time, record_count):
    self.station_code = station_code
    self.record_datetime = date_time
    self.record_count = record_count
    return

class shelve_stations_status(object):
  def __init__(self, use_logging, **kwargs):
    self.logger = None
    if use_logging:
      self.logger = logging.getLogger(__name__)
    self.data_connection = None
    self.filename = None

  def __del__(self):
    if self.data_connection:
      self.data_connection.close()

  def __getitem__(self, item):
    return self.get_station_rec(item)

  def __setitem__(self, station_code, rec):
    return self.set_station_rec(station_code, rec)

  def get_station_codes(self):
    keys = self.data_connection.keys()
    return keys

  def open(self, **kwargs):
    self.filename = kwargs['shelve_file']
    self.protocol = kwargs.get('protocol', 2)
    self.data_connection = shelve.open(self.filename, protocol=self.protocol)

  def close(self):
    if self.data_connection:
      self.data_connection.close()

  def save(self):
    self.data_connection.close()
    self.data_connection = shelve.open(self.filename, protocol=self.protocol)

  def get_station_rec(self, station_code):
    station_status_rec = None
    try:
      station_status_rec = self.data_connection[station_code]
    except (KeyError, AttributeError) as e:
      raise e
    return station_status_rec

  def set_station_rec(self, station_code, station_status_rec):
    self.data_connection[station_code] = station_status_rec

class shelve_stations_metadata(shelve_stations_status):
  def __init__(self, use_logging, **kwargs):
    shelve_stations_status.__init__(self, use_logging, **kwargs)

  def station_codes(self):
    for station_code in self.data_connection:
      yield station_code

class station_telemetry_statistic(shelve_stations_status):
  def __init__(self, use_logging, **kwargs):
    shelve_stations_status.__init__(self, use_logging, **kwargs)

  def set_station_rec(self, station_code, station_rec):
    try:
      telemetry_rec = self.data_connection[station_code]
    except KeyError,e:
      if self.logger:
        self.logger.exception(e)
      telemetry_rec = {}
      self.data_connection[station_code] = telemetry_rec
    except EOFError, e:
      if self.logger:
        self.logger.exception(e)
      telemetry_rec = {}
      self.data_connection[station_code] = telemetry_rec
    except Exception, e:
      if self.logger:
        self.logger.exception(e)
      telemetry_rec = {}
      self.data_connection[station_code] = telemetry_rec
    telemetry_rec = self.data_connection[station_code]
    telemetry_rec[station_rec.record_datetime] = station_rec

    self.data_connection[station_code] = telemetry_rec

"""
class station_data(object):
  def __init__(self, use_logging=True, **kwargs):
    self.logger = None
    if use_logging:
      self.logger = logging.getLogger(__name__)

    self.stations_metadata = None
    self.stations_status = None
    self.stations_telemetry_stats = None

  def open(self, **kwargs):
    try:
      if 'metadata_shelve_file' in kwargs:
        self.stations_metadata = shelve_stations_metadata(True)
        self.stations_metadata.open(shelve_file=kwargs['metadata_shelve_file'], protocol=2)
        self.metadata_filename = kwargs['metadata_shelve_file']
      if 'status_shelve_file' in kwargs:
        self.stations_status = shelve_stations_status(True)
        self.stations_status.open(shelve_file=kwargs['status_shelve_file'], protocol=2)
        self.status_filename = kwargs['status_shelve_file']
      if 'telemetry_shelve_file' in kwargs:
        self.stations_telemetry_stats = None
      return True
    except Exception, e:
      raise e
    return False

  def get_station_metadata_rec(self, station_code):
    return self.stations_metadata[station_code]
  def set_station_metadata_rec(self, station_code, metadata_rec):
    return self.stations_metadata.set_station_rec(station_code, metadata_rec)
  def station_codes(self):
    for station_code in self.data_connection:
      yield station_code
  def get_station_code_list(self):
    keys = self.stations_metadata.keys()
    keys.sort()
    return keys

  def get_station_status_rec(self, station_code):
    return self.stations_status[station_code]
  def set_station_status_rec(self, station_code, status_rec):
    return self.stations_status.set_station_rec(station_code, status_rec)
  def save_status_recs(self):
    self.stations_status.close()
    self.stations_status.open(shelve_file=self.status_filename, protocol=2)
"""
class stations_data(object):
  def __init__(self, use_logging=True):
    self.logger = None
    if use_logging:
      self.logger = logging.getLogger(__name__)

    self.stations = {}            #Dictionary keyed on station code that contains the metdata and status
    self.export_intervals = None  #Intervals when the telemetry exports data.
    self.error_list = []          #List of stations that haveI j status issues.

    self.stations_metadata_shelve = shelve_stations_metadata(True)
    self.stations_status_shelve = shelve_stations_status(True)
    self.station_telemetry_shelve = station_telemetry_statistic(True)

    self.current_check_status_time = None #Time that the test is running.
    self.all_stations_failed = False #Flag that specifies if all stations failed.
    self.all_east_stations_failed = False #Flag that specifies if all east stations failed.
    self.all_west_stations_failed = False #Flag that specifies if all west stations failed.
    self.email_interval_hours = 4 #Number of hours between emails.

  """
  Function: initialize_data_sources
  Purpose: Loads the stations metadata and current status from the shelve data sources.
  Parameters: A kwargs that contains:
    metadata_shelve_file - Full path to the metadata shelve
    status_shelve_file - Full path to the status shelve
  Return:
    True if successful, otherwise False.
  """
  def initialize_data_sources(self, **kwargs):
    #Load up the data from the shelve files.
    if 'metadata_shelve_file' in kwargs and 'status_shelve_file' in kwargs:
      self.stations_metadata_shelve.open(shelve_file=kwargs['metadata_shelve_file'], protocol=2)
      self.stations_status_shelve.open(shelve_file=kwargs['status_shelve_file'], protocol=2)
      for station_code in self.stations_metadata_shelve.station_codes():
        try:
         self.stations_status_shelve[station_code]
        except KeyError,e:
          if self.logger:
            self.logger.debug("Station: %s status does not exist, adding." % (station_code))
          station_status_rec = station_status(True, station_code=station_code)
          self.stations_status_shelve.set_station_rec(station_code, station_status_rec)
      #Force save of all the records.
      self.stations_status_shelve.save()

    if 'telemetry_stats_shelve_file' in kwargs:
      self.station_telemetry_shelve.open(shelve_file=kwargs['telemetry_stats_shelve_file'], protocol=2)

      self.export_intervals = [datetime.strptime("00:12:00", "%H:%M:%S"), datetime.strptime("00:42:00", "%H:%M:%S")]
      return True

    return False

  """
  Function: check_status
  Purpose:  For stations that should have transmitted up to the point in time we are checking, see
    if we have the expected data.
  Parameters: A kwargs that contains:
    save_settings_file -
    telemetry_export_directory - The directory where the telemetry export csv files are located.
      These are the files that we check for existence and data.
  Return:
    True if successful, otherwise False.
  """
  def check_status(self, **kwargs):
    #THis is the time that we use to filter for stations that should have transmitted.
    #The telemetry exports twice an hour and the ingestion program runs right after that.
    #This testing will occur before the ingestion and we use the now_time to check for stations
    #that should have transmitted.not
    self.current_check_status_time = datetime.utcnow()
    test_time = self.current_check_status_time.replace(hour = 0)

    bottom_hour = False
    if test_time.time() >= self.export_intervals[0].time() and test_time.time() < self.export_intervals[1].time():
      bottom_hour = True

    stations_failed_count = 0
    stations_checked_count = 0
    east_station_check_count = 0
    west_station_check_count = 0
    goes_telemetered_check_count = 0
    east_fail_count = 0
    west_fail_count = 0
    for station_code in self.stations_metadata_shelve.station_codes():
      check_for_data = False
      station_metadata_rec = self.stations_metadata_shelve[station_code]
      #If we have an export time, the station is telemetered. We need to determine
      #based on the time this check is running what stations should have created telemetry files.
      transmit_time = ""
      if station_metadata_rec.transmit_time:
        transmit_time = station_metadata_rec.transmit_time.strftime("%H:%M:%S")
        export_time = station_metadata_rec.export_time.strftime("%H:%M:%S")
      if self.logger:
        self.logger.debug("Station: %s Transmit Time: %s Export Time: %s" % (station_code, transmit_time, export_time))

      if station_metadata_rec.export_time is not None:
        if bottom_hour and station_metadata_rec.export_time == self.export_intervals[0] or\
          not bottom_hour and station_metadata_rec.export_time == self.export_intervals[1]:
          check_for_data = True

      #Non GOES stations will always be checked.
      else:
        check_for_data = True

      if check_for_data:
        stations_checked_count += 1
        #We count the goes stations processed so we can send out an immediate alert if they
        #all fail. Previously I was counting every station, however we are only required to
        #monitor the GOES telemetry not the other data pulls.
        if station_metadata_rec.hads_id is not None:
          goes_telemetered_check_count += 1
          #Keep track of the GOES East and West stations. If all of one or the other
          #fail, we send alert
          if station_metadata_rec.goes_satellite == 'E':
            east_station_check_count += 1
          elif station_metadata_rec.goes_satellite == 'W':
            west_station_check_count += 1

        if self.logger:
          self.logger.debug("Station: %s (%s) checking for telemetry." % (station_code, station_metadata_rec.goes_satellite))
        #station_status_rec = self.stations[station_code]['status']
        station_status_rec = self.stations_status_shelve[station_code]

        if station_status_rec.check_status(telemetry_export_directory=kwargs['telemetry_export_directory'],
                                            status_check_time=self.current_check_status_time,
                                            min_recs_per_hour=station_metadata_rec.min_recs_per_hour):

          #We're only going to send out an alert if the number of misses exceeds the allowable number.
          #if (station_status_rec.get_current_hour_count_missed() % station_status_rec.get_allowed_hours_to_miss()) == 0:
          if station_status_rec.get_current_hour_count_missed() >= station_status_rec.get_allowed_hours_to_miss():
            self.error_list.append(station_code)
            if self.logger:
              self.logger.debug(station_status_rec.decode_current_status())

          #Keep a running count of how many station failure we have, if all the expected to report stations fail,
          #we need to report this.
          stations_failed_count += 1
          #Keep track of East and West GOES fail counts.
          if station_metadata_rec.hads_id is not None:
            if station_metadata_rec.goes_satellite == 'E':
              east_fail_count += 1
            elif station_metadata_rec.goes_satellite == 'W':
              west_fail_count += 1
            else:
              i=0
        else:
          if self.logger:
            self.logger.debug("Station: %s @ %s is transmitting properly." % (station_code, station_status_rec.last_update_time.strftime("%Y-%m-%d %H:%M:%S")))

        #Update the shelve.
        self.stations_status_shelve[station_code] = station_status_rec
        if station_status_rec.get_last_update_time() != None:
          telemetry_stat = station_telemetry_stat(True)
          telemetry_stat.set_statistic(station_code, station_status_rec.get_last_update_time(), station_status_rec.current_telemetry_dates_cnt)
          self.station_telemetry_shelve.set_station_rec(station_code, telemetry_stat)

    #if stations_failed_count >= goes_telemetered_check_count or\
    #We keep track of just the GOES telemetered failures of when either we miss all east, west, or all of
    #both.
    if (east_fail_count + west_fail_count) >= goes_telemetered_check_count or\
      east_fail_count >= east_station_check_count or\
      west_fail_count >= west_station_check_count:
      fail_code = []
      if (east_fail_count + west_fail_count) >= goes_telemetered_check_count:
        self.all_stations_failed = True
        fail_code.append("stations")
      else:
        if east_fail_count >= east_station_check_count:
          self.all_east_stations_failed = True
          fail_code.append("east stations")
        if west_fail_count >= west_station_check_count:
          self.all_west_stations_failed = True
          fail_code.append("west stations")
      if self.logger:
        self.logger.error("All %s status checked failed." % (",".join(fail_code)))

    self.stations_status_shelve.save()


  def output_results(self, **kwargs):
    if self.logger:
      self.logger.debug("Exporting test results to: %s" % (kwargs['report_out_filename']))
    report_out_file = None
    try:
      report_out_file = open(kwargs['report_out_filename'], 'w')
    except IOError,e:
      if self.logger:
        self.logger.exception(e)
    try:
      eastern = timezone('US/Eastern')
      mytemplate = Template(filename=kwargs['report_template'])

      station_failure_list = []
      if 'report_all_failures' in kwargs and kwargs['report_all_failures']:
        for station_code in self.stations_metadata_shelve.station_codes():
          station_status_rec = self.stations_status_shelve[station_code]
          if station_status_rec.get_current_hour_count_missed() >= station_status_rec.get_allowed_hours_to_miss():
            station_failure_list.append(station_code)
      else:
        station_failure_list = self.error_list
      station_failure_list.sort()

      #Call out the specific failure receiver E or W or ALL if they all missed.
      goes_hemisphere_fail = []
      if self.all_stations_failed:
        goes_hemisphere_fail.append('ALL')
      else:
        if self.all_east_stations_failed:
          goes_hemisphere_fail.append('EAST')
        if self.all_west_stations_failed:
          goes_hemisphere_fail.append('WEST')

      goes_hemisphere_fail = ",".join(goes_hemisphere_fail)
      #Get the time the test was run.
      est_time = timezone('UTC').localize(self.current_check_status_time).astimezone(eastern)
      template_output = mytemplate.render(test_time=est_time.strftime("%Y-%m-%d %H:%M:%S"),
                                          station_codes=station_failure_list,
                                          station_data={'metadata' : self.stations_metadata_shelve,
                                                        'status' : self.stations_status_shelve},
                                          goes_hemisphere_failed=goes_hemisphere_fail,
                                          tz_obj=eastern)
    except:
      if self.logger:
        self.logger.exception(makoExceptions.text_error_template().render())
    #Write the html file so we can always browse the latest test results. We only send the
    #email if we have stations in the list.
    if report_out_file is not None:
      report_out_file.write(template_output)
      report_out_file.close()
      #Only send the email if we have stations to report.
      try:
        #Only send an email on the specified interval or if all stations failed to report.
        if (est_time.hour % kwargs['email_interval_hours']) == 0 and est_time.minute < 30\
          or (self.all_stations_failed or self.all_east_stations_failed or self.all_west_stations_failed):
          if self.logger:
            self.logger.debug("Emailing test results to: %s" % (kwargs['send_to']))
          if ('email_host' in kwargs) and\
            (self.all_stations_failed or self.all_east_stations_failed or self.all_west_stations_failed or len(self.error_list)):
            subject = "[CDMO] Telemetry Alerts"
            if self.all_stations_failed or self.all_east_stations_failed or self.all_west_stations_failed:
              subject = "[CDMO ERROR] %s STATIONS FOR TIME SLOT FAILED TO REPORT" % (goes_hemisphere_fail)

            email_obj = smtpClass(host=kwargs['email_host'], user=kwargs['email_user'], password=kwargs['email_password'])
            email_obj.from_addr("%s@%s" % (kwargs['email_from_addr'], kwargs['email_host']))
            email_obj.rcpt_to(kwargs['send_to'])
            email_obj.message(template_output)
            email_obj.subject(subject)
            email_obj.send(content_type="html")
        #If all the stations missed, send out a text as well.
        if (self.all_stations_failed or self.all_east_stations_failed or self.all_west_stations_failed) and kwargs['text_only_on_all_misses']:
          if self.logger:
            self.logger.debug("All stations missed, sending texts to: %s" % (kwargs['text_addresses']))
          email_obj.rcpt_to(kwargs['text_addresses'])
          email_obj.subject("[CDMO]%s STATIONS MISSED" % (goes_hemisphere_fail))
          email_obj.message("%s STATIONS MISSED, TELEMETRY MAY BE DOWN." % (goes_hemisphere_fail))
          email_obj.send()

      except Exception, e:
        if self.logger:
          self.logger.exception(e)

    if self.logger:
      self.logger.debug("Finished output_results.")

    return

  """
  Function: save_status
  Purpose: For any changes to the status records, this saves them to the shelve.
  Parameters: None
  Return: True if succesful, otherwise False.
  """
  def update_status(self, station_code, status_rec):
    ret_val = True
    if self.logger:
      self.logger.debug("Begin saving station: %s status." % (station_code))

    self.stations_status_shelve[station_code] = status_rec
    self.stations_status_shelve.save()
    if self.logger:
      self.logger.debug("Finished saving station: %s status." % (station_code))

  def get_station_codes(self):
    return
  def station_codes(self):
    for station_code in self.stations_metadata_shelve.station_codes():
      yield station_code

  def get_station_data(self, station_code):
    status = None
    statistics = None
    try:
      metadata = self.stations_metadata_shelve[station_code]
    except (KeyError,AttributeError) as e:
      if self.logger:
        self.logger.exception(e)
    else:
      try:
        status = self.stations_status_shelve[station_code]
      except (KeyError,AttributeError) as e:
        if self.logger:
          self.logger.exception(e)
      try:
        statistics = self.station_telemetry_shelve[station_code]
      except (KeyError,AttributeError) as e:
        if self.logger:
          self.logger.exception(e)
      return {'metadata': metadata,
              'status': status,
              'statistics': statistics}

    return None

  """
  Function: update_station_metadata
  Purpose: Harvests the station data from the sample stations file and the telemetry data from the
    XC_SITES.csv file.
  Parameters:
    kwargs that contain:
      sample_stations_file - Full path to the sample_stations.csv file.
      telemetry_setup_file - Full path to the XC_SITES.csv file.
      non_goes_telemetry_setup_file - Full path to the non GOES csv file.
  """
  def update_station_metadata(self, **kwargs):
    self.stations_metadata_shelve = shelve_stations_metadata(True)
    self.stations_metadata_shelve.open(shelve_file=kwargs['metadata_shelve_file'], protocol=2)

    if self.load_sample_stations_file(kwargs['sample_stations_file']):
      if self.load_station_telemetry_setup(kwargs['telemetry_setup_file']):
        if self.load_non_goes_telemetry_setup(kwargs['non_goes_telemetry_setup_file']):

          #Mark the stations either Goes East or West.
          station_code_keys = self.stations_metadata_shelve.get_station_codes()
          for station_id in station_code_keys:
            metadata_rec = self.stations_metadata_shelve[station_id]
            if station_id in kwargs['west_station_list']:
              metadata_rec.goes_satellite = 'W'
            else:
              metadata_rec.goes_satellite = 'E'
            self.stations_metadata_shelve[station_id] = metadata_rec

          #Save the data.
          self.stations_metadata_shelve.save()
    return

  def load_non_goes_telemetry_setup(self, file_name):
    ret_val = False
    header_row = [
      "STATION_ID",
      "REPORTING_TIME",
      "RECORDS_PER_HOUR"
    ]
    try:
      if self.logger:
        self.logger.debug("Start reading non goes telemetry info file: %s" % (file_name))

      telemetry_metadata_file = open(file_name, "rU")
      dict_file = csv.DictReader(telemetry_metadata_file, delimiter=',', quotechar='"', fieldnames=header_row)
    except IOError,e:
      if self.logger:
        self.logger.exception(e)
    else:
      #get the transmit time from the file.
      line_num = 0
      for row in dict_file:
        if line_num > 0:
          station_id = row['STATION_ID'].lower()
          if station_id in self.stations_metadata_shelve.station_codes():
            metadata_rec = self.stations_metadata_shelve[station_id]
            metadata_rec.transmit_time = datetime.strptime(row['REPORTING_TIME'], '%H:%M:%S')
            metadata_rec.export_time = ""
            #if metadata_rec.transmit_time.time() <= datetime.strptime('00:12:00', "%H:%M:%S").time() or\
            #  metadata_rec.transmit_time.time() > datetime.strptime('00:42:00', "%H:%M:%S").time():
            if metadata_rec.transmit_time.time() <= datetime.strptime('00:10:40', "%H:%M:%S").time() or\
              metadata_rec.transmit_time.time() > datetime.strptime('00:40:40', "%H:%M:%S").time():
              metadata_rec.export_time = datetime.strptime('00:12:00', "%H:%M:%S")
            else:
              metadata_rec.export_time = datetime.strptime('00:42:00', "%H:%M:%S")
            metadata_rec.min_recs_per_hour = int(row['RECORDS_PER_HOUR'])
            #Save the updated rec back to store.
            self.stations_metadata_shelve[station_id] = metadata_rec
          else:
            if self.logger:
              self.logger.error("Station: %s not found in stations metadata." % (station_id))

        line_num += 1

      telemetry_metadata_file.close()
      ret_val =  True

    if self.logger:
      self.logger.debug("Finish reading non goes telemetry info file: %s" % (file_name))
    return ret_val

  def load_station_telemetry_setup(self, file_name):
    ret_val = False
    header_row = [
      "STATION_ID",
      "ENABLED",
      "UNIT_ID",
      "SITE_COMMENT",
      "INSTALLATION_DATE",
      "AGENCY",
      "COUNTRY",
      "DISTRICT",
      "CITY",
      "STATE",
      "COUNTY",
      "BASIN",
      "LATITUDE",
      "LONGITUDE",
      "ELEVATION",
      "COEFFICIENT1",
      "COEFFICIENT2",
      "COEFFICIENT3",
      "WEIGHTING_FACTOR",
      "SATELLITE_ID",
      "PRIMARY_CHANNEL",
      "RANDOM_CHANNEL",
      "REPORTING_TIME",
      "REPORTING_INTERVAL",
      "LAST_UPDATE",
      "TZONE_CODE",
      "GMT_OFFSET",
      "SHEF_ID",
      "ALTERNATE_CHAR_ID_1",
      "ALTERNATE_CHAR_ID_2",
      "GPSTRING1",
      "GPSTRING2",
      "GPNUMBER1",
      "GPNUMBER2",
      "SETUP_FILE_NAME",
      "PICTURE_FILE_NAME"
    ]
    try:
      if self.logger:
        self.logger.debug("Start reading telemetry info file: %s" % (file_name))

      telemetry_metadata_file = open(file_name, "rU")
      dict_file = csv.DictReader(telemetry_metadata_file, delimiter=',', quotechar='"', fieldnames=header_row)
    except IOError,e:
      if self.logger:
        self.logger.exception(e)
    else:
      #get the transmit time from the file.
      line_num = 0
      for row in dict_file:
        if line_num > 0:
          station_id = row['STATION_ID'].lower()
          if station_id in self.stations_metadata_shelve.station_codes():
            metadata_rec = self.stations_metadata_shelve[station_id]
            metadata_rec.transmit_time = datetime.strptime(row['REPORTING_TIME'], '%H:%M:%S')
            #Transmit times are only minute/second reference for each hour, so if we have
            #any actual hour values, set them to 0.
            if metadata_rec.transmit_time.hour != 0:
              metadata_rec.transmit_time = metadata_rec.transmit_time.replace(hour=0)
            metadata_rec.transmit_channel = int(row['PRIMARY_CHANNEL'])
            #The telemetry decoder does not create the CSV files at the time of decoding, there are
            #a couple of windows it uses. Based on the transmit time, we need to assign the export_time.
            #Currently we have one at 00:12:00 and 00:42:00
            metadata_rec.export_time = ""
            #if metadata_rec.transmit_time.time() < datetime.strptime('00:12:00', "%H:%M:%S").time() or\
            #  metadata_rec.transmit_time.time() > datetime.strptime('00:42:00', "%H:%M:%S").time():
            if metadata_rec.transmit_time.time() <= datetime.strptime('00:10:40', "%H:%M:%S").time() or\
              metadata_rec.transmit_time.time() > datetime.strptime('00:40:40', "%H:%M:%S").time():
              metadata_rec.export_time = datetime.strptime('00:12:00', "%H:%M:%S")
            else:
              metadata_rec.export_time = datetime.strptime('00:42:00', "%H:%M:%S")
            #Now save the updated rec back.
            self.stations_metadata_shelve[station_id] = metadata_rec
          else:
            if self.logger:
              self.logger.error("Station: %s not found in stations metadata." % (station_id))

        line_num += 1

      telemetry_metadata_file.close()
      ret_val = True

    if self.logger:
      self.logger.debug("Finish reading telemetry info file: %s" % (file_name))

    return ret_val

  def load_sample_stations_file(self, file_name):
    ret_val = True
    header_row = [
      "Row",
      "NERR Site ID ",
      "Station Code",
      "Station Name",
      "Lat Long",
      "Latitude ",
      "Longitude",
      "Status",
      "Active Dates",
      "State",
      "Reserve Name",
      "Real Time",
      "HADS ID",
      "GMT Offset",
      "Station Type",
      "Region",
      "isSWMP"
    ]
    try:
      if self.logger:
        self.logger.debug("Start reading reserve info file: %s" % (file_name))

      reserve_file = open(file_name, "r")
      dict_file = csv.DictReader(reserve_file, delimiter=',', quotechar='"', fieldnames=header_row)
    except IOError,e:
      if self.logger:
        self.logger.exception(e)
    else:
      try:
        line_num = 0
        for row in dict_file:
          if line_num > 0:
            station_type = int(row["Station Type"].strip())
            #0 = met, 1 = wq, 2 = nut, no telemetry.
            if station_type == 0 or station_type == 1:
              station_code = row["Station Code"].strip()
              if station_code not in self.stations:
                #Make sure the station is real time.
                if row["Real Time"].strip() == 'R':
                  if self.logger:
                    self.logger.debug("Adding station: %s" % (station_code))

                  self.stations_metadata_shelve[station_code] = station_metadata(True,
                                                             state=row["State"].strip().upper(),
                                                             reserve_name=row["Reserve Name"].strip(),
                                                             station_code=row["Station Code"].strip(),
                                                             station_name=row["Station Name"].strip(),
                                                             reserve_code=row["NERR Site ID "].strip().upper(),
                                                             longitude=row["Longitude"].strip(),
                                                             latitude=row["Latitude "].strip(),
                                                             active_dates=row["Active Dates"].strip(),
                                                             hads_id=row["HADS ID"].strip(),
                                                             station_type=station_type,
                                                             region=row["Region"].strip(),
                                                             is_swmp=row["isSWMP"].strip())
                else:
                  if self.logger:
                    self.logger.debug("Station: %s is not real time." % (station_code))
          line_num += 1
        if self.logger:
          self.logger.debug("%d station info processed" % (len(self.stations)))

        reserve_file.close()

        ret_val = True

      except Exception, e:
        if self.logger:
          self.logger.exception(e)

    if self.logger:
      self.logger.debug("Finish reading reserve info file: %s" % (file_name))

    return ret_val

  def write_json_data(self, **kwargs):
    json_outfile = kwargs['json_out_file']
    stations_data = {}
    eastern = timezone('US/Eastern')
    station_code_keys = self.stations_metadata_shelve.get_station_codes()
    station_code_keys.sort()
    for station_code in station_code_keys:
      #status_dict = self.stations[station_code]['status'].to_dict(eastern)
      status_rec = self.stations_status_shelve[station_code]
      status_dict = status_rec.to_dict(eastern)

      status_dict['status_field_text'] = ",".join(status_rec.decode_current_status())
      stations_data[station_code] = {'metadata': self.stations_metadata_shelve[station_code].to_dict(),
                                     'status': status_dict}
    try:
      with open(json_outfile, "w") as json_file:
        json_file.write(simplejson.dumps(stations_data, sort_keys=True, indent=2 * ' '))
    except IOError,e:
      if self.logger:
        self.logger.exception(e)

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="configFile",
                    help="Configuration file" )
  parser.add_option("-u", "--UpdateStationMetadata", dest="update_station_metadata", default=False, action="store_true",
                    help="Configuration file" )
  parser.add_option("-s", "--CheckStatus", dest="check_status", default=False, action="store_true",
                    help="Configuration file" )
  parser.add_option("-j", "--BuildJSON", dest="build_json_file", default=False, action="store_true",
                    help="Flag that specifies build the JSON output file." )
  parser.add_option("-f", "--SaveAllStatus", dest="save_all_status", default=False, action="store_true",
                    help="Flag that specifies to load and re-save the status, utility when we add a new field and want to keep current settings." )
  parser.add_option("-r", "--GenerateReport", dest="gen_report", default=False, action="store_true",
                    help="")
  (options, args) = parser.parse_args()

  configFile = ConfigParser.RawConfigParser()
  configFile.read(options.configFile)

  logger = None
  logFile = configFile.get('logging', 'configfile')
  if logFile:
    logging.config.fileConfig(logFile)
    logger = logging.getLogger("telemetry_alert_logging")
    logger.info("Log file opened.")


  try:
    status_shelve_file = configFile.get('settings', 'status_shelve_file')
    metadata_shelve_file = configFile.get('settings', 'metadata_shelve_file')
    telemetry_stats_shelve_file = configFile.get('settings', 'telemetry_stats_shelve_file')
    export_file_directory = configFile.get('telemetry_settings', 'export_file_directory')
    report_template = configFile.get('template_settings', 'report_template')
    report_out_filename = configFile.get('template_settings', 'report_out_filename')
    json_out_file = configFile.get('json_settings', 'json_outfile')

  except ConfigParser.NoOptionError,e:
    if logger:
      logger.exception(e)
  else:
    data = stations_data()
    if options.update_station_metadata:
      try:
        telemetry_metadata_file = configFile.get('telemetry_settings', 'data_file')
        non_goes_telemetry_metadata_file = configFile.get('telemetry_settings', 'non_goes_data_file')
        local_stations_file = configFile.get('station_data_settings', 'sample_stations_file')
        west_station_list = configFile.get('telemetry_settings', 'west_stations').split(',')
      except ConfigParser.NoOptionError,e:
        if logger:
          logger.exception(e)
      else:
        data.update_station_metadata(sample_stations_file=local_stations_file,
                                   telemetry_setup_file=telemetry_metadata_file,
                                   non_goes_telemetry_setup_file=non_goes_telemetry_metadata_file,
                                   metadata_shelve_file=metadata_shelve_file,
                                   west_station_list=west_station_list)
    if options.check_status:
      try:
        email_host = configFile.get('email_settings', 'server')
        email_user = configFile.get('email_settings', 'user_name')
        email_password = configFile.get('email_settings', 'password')
        email_from_addr = configFile.get('email_settings', 'from_addr')
        send_to = configFile.get('email_settings', 'send_to').split(',')
        email_interval_hours = configFile.getint('email_settings', 'email_interval_hours')
        text_only_on_all_misses = configFile.getboolean('text_settings', 'text_only_on_all_misses')
        text_addresses = configFile.get('text_settings', 'text_addresses').split(',')
      except ConfigParser.NoOptionError,e:
        if logger:
          logger.exception(e)

      data.initialize_data_sources(metadata_shelve_file=metadata_shelve_file,
                                    status_shelve_file=status_shelve_file,
                                    telemetry_stats_shelve_file=telemetry_stats_shelve_file)
      data.check_status(telemetry_export_directory=export_file_directory)

      data.output_results(report_template=report_template,
                          report_out_filename=report_out_filename,
                          email_host=email_host,
                          email_user=email_user,
                          email_password=email_password,
                          email_from_addr=email_from_addr,
                          send_to=send_to,
                          email_interval_hours=email_interval_hours,
                          report_all_failures=True,
                          text_only_on_all_misses=text_only_on_all_misses,
                          text_addresses=text_addresses)

      data.write_json_data(json_out_file=json_out_file)

    if options.build_json_file or options.save_all_status:
      data.initialize_data_sources(metadata_shelve_file=metadata_shelve_file,
                                    status_shelve_file=status_shelve_file)
      if options.build_json_file:
        data.write_json_data(json_out_file=json_out_file)

      #if options.save_all_status:
      #  data.save_status(save_all=True)
  if logger:
    logger.info("Log file closed.")

  return

if __name__ == '__main__':
  main()
