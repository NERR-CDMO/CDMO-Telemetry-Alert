import sys
#sys.path.append('/Users/danramage/Documents/workspace/CDMO/CDMO-Telemetry-Alert')
sys.path.append("D:\scripts\common")

import logging.config
import optparse
import ConfigParser
import urllib
import simplejson

from telemetry_alerts import stations_data, station_metadata, station_status

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="configFile",
                    help="Configuration file" )
  parser.add_option("-j", "--JsonData", dest="json_data",
                    help="JSON data that represents the stations with changed params.")
  (options, args) = parser.parse_args()

  configFile = ConfigParser.RawConfigParser()
  configFile.read(options.configFile)

  ret_val = "Failed to save changes."
  logger = None
  logFile = configFile.get('logging', 'web_handler_config')
  if logFile:
    logging.config.fileConfig(logFile)
    logger = logging.getLogger("telemetry_alert_logging")
    logger.info("Log file opened.")

  json_string = urllib.unquote(options.json_data)
  #json_string = '{"cbmjbmet":{"test_disabled":false,"allowed_hour_count_to_miss":4}}'
  if logger:
    logger.debug("JSON Data: %s" % (json_string))
  try:
    json_data = simplejson.loads(json_string)
  except Exception,e:
    if logger:
      logger.exception(e)
  else:
    try:
      metadata_shelve_file = configFile.get('settings', 'metadata_shelve_file')
      status_shelve_file = configFile.get('settings', 'status_shelve_file')
      telemetry_stats_shelve_file = configFile.get('settings', 'telemetry_stats_shelve_file')
      json_out_file = configFile.get('json_settings', 'json_outfile')
    except ConfigParser.NoOptionError,e:
      if logger:
        logger.exception(e)
    else:
      data = stations_data()
      data.initialize_data_sources(metadata_shelve_file=metadata_shelve_file,
                                      status_shelve_file=status_shelve_file,
                                      telemetry_stats_shelve_file=telemetry_stats_shelve_file)
      for station_code in json_data:
        ascii_code = station_code.encode("ascii")
        if logger:
          logger.debug("Station: %s saving data." % (ascii_code))
        try:

          station_data = data.get_station_data(ascii_code)

          if station_data['status'].get_allowed_hours_to_miss() != json_data[station_code]['allowed_hour_count_to_miss']:
            if logger:
              logger.debug("Saving allowed hours to miss %d." % (json_data[station_code]['allowed_hour_count_to_miss']))
            station_data['status'].set_allowed_hours_to_miss(json_data[station_code]['allowed_hour_count_to_miss'])

          if station_data['status'].is_test_disabled() != json_data[station_code]['test_disabled']:
            if logger:
              logger.debug("Saving disable test flag %d." % (json_data[station_code]['test_disabled']))
            station_data['status'].set_disable_test(json_data[station_code]['test_disabled'])

          ret_val = "Successfully saved changes."

          data.update_status(ascii_code, station_data['status'])
        except Exception, e:
          if logger:
            logger.exception(e)

      data.write_json_data(json_out_file=json_out_file)

  if logger:
    logger.info("Log file closed.")

  print ret_val

if __name__ == '__main__':
  main()
