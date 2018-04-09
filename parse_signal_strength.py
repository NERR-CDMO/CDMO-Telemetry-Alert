import os
import sys
#sys.path.append('/Users/danramage/Documents/workspace/CDMO/CDMO-Telemetry-Alert')
#sys.path.append("D:\scripts\common")
sys.path.append("../commonfiles/python")

import ConfigParser
from datetime import datetime, timedelta
import logging.config
import optparse
import csv
from CDMO_Util_Classes import sample_stations_file
from telemetry_alerts import shelve_stations_status

class signal_data:
  def __init__(self, **kwargs):
    self.station_code = kwargs.get('station_code', None)
    self.record_datetime = kwargs.get('record_datetime', None)
    self.signal_strength = kwargs.get('signal_strength', 0)
    self.message_length = kwargs.get('message_length', 0)

  def __getstate__(self):
    d = dict(self.__dict__)
    return d

  def __setstate__(self, state):
    self.__dict__ = state

  def set_signal_data(self, **kwargs):
    self.station_code = kwargs['station_code']
    self.record_datetime = kwargs['date_time']
    self.signal_strength = kwargs['signal_strength']
    self.message_length = kwargs['message_length']
    return

class station_signal_data(shelve_stations_status):
  #def __init__(self, use_logging, **kwargs):
  #  shelve_stations_status.__init__(self, use_logging, **kwargs)

  def set_station_rec(self, station_code, station_rec):
    try:
      signal_rec = self.data_connection[station_code]
    except (KeyError,EOFError) as e:
      if self.logger:
        self.logger.exception(e)
        signal_rec = {}
      self.data_connection[station_code] = signal_rec
    except Exception, e:
      raise e
    try:
      signal_rec = self.data_connection[station_code]
      signal_rec[station_rec.record_datetime] = station_rec
      self.data_connection[station_code] = signal_rec
    except Exception, e:
      if self.logger:
        self.logger.exception(e)


  def get_station_rec(self, station_code):
    try:
      station_status_rec = self.data_connection[station_code]
      return station_status_rec
    except (KeyError, AttributeError) as e:
      raise e


def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="Configuration file", default=None),
  parser.add_option("-o", "--SignalStrengthFile", dest="sig_file",
                    help="Signal Strength File" )
  parser.add_option("-e", "--DaysToKeep", dest="days_to_keep", default=None, type=int,
                    help="Number of days of history to keep" )

  (options, args) = parser.parse_args()

  config_file = ConfigParser.RawConfigParser()
  config_file.read(options.config_file)


  try:
    logger = None
    log_file = config_file.get('logging', 'signal_strength')
    logging.config.fileConfig(log_file)
    logger = logging.getLogger("signal_logging")
    logger.info("Log file opened.")

    try:
      shelve_file = config_file.get('shelve_files', 'signal_strength_shelve_file')
      stations_url = config_file.get('stations_metadata', 'remote_stations_file')
      stations_file_dest = config_file.get('stations_metadata', 'status_config_directory')
    except ConfigParser.NoOptionError, e:
      if logger:
        logger.exception(e)
    else:
      signal_shelve = station_signal_data(True)
      signal_shelve.open(shelve_file=shelve_file, protocol=2)
      now_time = datetime.now()
      if options.days_to_keep is not None:
        seconds_in_day = 3600 * 24
        stations = signal_shelve.get_station_codes()
        for station in stations:
          station_rec = signal_shelve.get_station_rec(station)
          delete_list = []
          for date_rec in station_rec:
            #date_rec = datetime.strptime(date_rec, "%Y-%m-%d %H:%M:%S")
            time_delta = now_time - date_rec
            if int(time_delta.total_seconds() / seconds_in_day) > options.days_to_keep:
              delete_list.append(date_rec)
          for delete_item in delete_list:
            del station_rec[delete_item]

      with open(options.sig_file, 'r') as signal_strength_file:
        signal_strength_reader = csv.reader(signal_strength_file, delimiter=',', quotechar='"')
        samp_stations = sample_stations_file(use_logging=True)
        save_path = os.path.join(stations_file_dest, 'stations_metadata.csv')
        samp_stations.download_file(stations_url, save_path)
        samp_stations.open(save_path)

        for row in signal_strength_reader:
          station_code = row[20]
          rec_date, rec_time = row[18].split('  ')
          rec_date = rec_date.split('/')
          rec_date = "%02d/%02d/%4d" % (int(rec_date[0]), int(rec_date[1]), int(rec_date[2]))

          if rec_time.find('PM') != -1:
            rec_time = rec_time.split('PM')[0]
            ampm = 'PM'
          else:
            rec_time = rec_time.split('AM')[0]
            ampm = 'AM'

          rec_time = rec_time.split(':')
          rec_time = '%02d:%02d:%02d %s' % (int(rec_time[0]), int(rec_time[1]), int(rec_time[2]), ampm)

          rec_date = '%s %s' % (rec_date, rec_time)
          rec_date = datetime.strptime(rec_date, "%m/%d/%Y %I:%M:%S %p")

          data = signal_data(station_code=station_code.upper(),
                             record_datetime=rec_date,
                             signal_strength=int(float(row[22])),
                             message_length=int(float(row[27])))
          logger.debug("Station: %s Date: %s SigStr: %d Len: %d" % (data.station_code,
                                                                    data.record_datetime,
                                                                    data.signal_strength,
                                                                    data.message_length))
          signal_shelve.set_station_rec(data.station_code, data)
        signal_shelve.save()

  except (IOError, Exception) as e:
    if logger is not None:
      logger.exception(e)
    else:
      import traceback
      traceback.print_exc(e)
  logger.debug("Closing log.")
  return

if __name__ == '__main__':
  main()
