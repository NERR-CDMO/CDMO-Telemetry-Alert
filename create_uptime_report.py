import sys
#sys.path.append("/Users/danramage/Documents/workspace/CDMO/python/common")
sys.path.append("D:\scripts\common")
import logging.config
import optparse
import ConfigParser
from datetime import datetime, timedelta
from pytz import timezone

from mako.template import Template
from mako import exceptions as makoExceptions

from telemetry_alerts import stations_data, station_metadata, station_status, station_telemetry_stat


def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="configFile",
                    help="Configuration file" )
  parser.add_option("-t", "--ReportTemplate", dest="report_template",
                    help="Template to use for the report." )
  parser.add_option("-f", "--PageFile", dest="page_file",
                    help="File to write the HTML report to." )
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
  except ConfigParser.NoOptionError,e:
    if logger:
      logger.exception(e)
  else:
    data = stations_data()
    data.initialize_data_sources(metadata_shelve_file=metadata_shelve_file,
                                status_shelve_file=status_shelve_file,
                                telemetry_stats_shelve_file=telemetry_stats_shelve_file)
    hours = 24
    today = datetime.utcnow()
    start_date = today
    end_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(hours=hours)
    eastern_tz = timezone("US/Eastern")
    station_stats = {}
    codes = data.stations_metadata_shelve.get_station_codes()
    codes.sort()
    for station_code in codes:
      station_rec = data.get_station_data(station_code)
      if station_rec['statistics'] != None:
        telem_stats = station_rec['statistics']
        telem_dates = telem_stats.keys()
        telem_dates.sort()
        date_count = 0
        for telem_date in telem_dates:
          if telem_date >= start_date and telem_date < end_date:
            date_count += 1
        percentage = int((float(date_count) / float(hours)) * 100)
      else:
        percentage = 0
      station_stats[station_code] = percentage

    try:
      if logger:
        logger.debug("Opening template file: %s" % (options.report_template))
      mytemplate = Template(filename=options.report_template)
      #Get the time the test was run.
      #est_time = timezone('UTC').localize(self.current_check_status_time).astimezone(eastern)
      times = [start_date, end_date]
      template_output = mytemplate.render(times=times,
                                          station_codes=codes,
                                          station_stats=station_stats
                                          )

    except:
      if logger:
        logger.exception(makoExceptions.text_error_template().render())
    #Write the html file so we can always browse the latest test results. We only send the
    #email if we have stations in the list.
    try:
      if logger:
        logger.debug("Opening report file: %s" % (options.page_file))
      with open(options.page_file, "w") as report_out_file:
        report_out_file.write(template_output)
    except IOError, e:
      if logger:
        logger.exception(e)
  if logger:
    logger.info("Log file closed.")

if __name__ == '__main__':
  main()



