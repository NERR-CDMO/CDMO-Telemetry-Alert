<!DOCTYPE html>

<html lang="en">
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link href="http://cdmo.baruch.sc.edu/resources/js/bootstrap/bootstrap-3.3.4/css/bootstrap.min.css" rel="stylesheet">
      <link href="http://cdmo.baruch.sc.edu/resources/js/bootstrap/bootstrap-3.3.4/css/bootstrap-theme.min.css" rel="stylesheet">
      <script type="application/javascript" src="http://cdmo.baruch.sc.edu/resources/js/jquery/jquery-1.11.1.min.js"></script>
      <script type="application/javascript" src="http://cdmo.baruch.sc.edu/resources/js/bootstrap/bootstrap-3.3.4/js/bootstrap.min.js"></script>


      <title>NERRS Telemetry Miss Report</title>
    </head>
    <body>
        <div class="container">
            <div class="row">
                <h1>NERRS Telemetry Misses Report</h1>
                <h2>Report Period: ${test_time}</h2>
                <h3>These are the stations that have missed transmissions within their alloted time.</h3>
                </br>
            </div>
            <div class="row">
                % if len(goes_hemisphere_failed) == 0:
                    <table class="table table-striped table-bordered">
                        <tr>
                            <th>Station Code</th>
                            <th>Hours Missed</th>
                            <th>Last Update</th>
                            <th>Issues</th>
                        </tr>
                        % for station_code in station_codes:
                        <tr>
                            <td>
                                ${ station_code }
                            </td>
                            <td>
                                ${ station_data['status'][station_code].current_hour_count_missed }
                            </td>
                            <td>
                              ${station_data['status'][station_code].get_last_update_time_string(tz_obj)}
                            </td>
                            <td>
                              ${ ",".join(station_data['status'][station_code].decode_current_status()) }
                            </td>
                        </tr>
                        % endfor
                    </table>
                % else:
                  <div>
                    <h2><p class="text-danger">No ${goes_hemisphere_failed} stations reported this period, check telemetry</p></h2>
                  </div>
                % endif
            </div>
          <div class="row">
            <div class="col-xs-12">
                <p>
                    To see all stations reporting status, click here: http://cdmo.baruch.sc.edu/telemetry_alerts/telemetry_alerts.html
                </p>
                <p>
                    To see current status report, click here: http://cdmo.baruch.sc.edu/telemetry_alerts/data/telemetry_check_results.html
                </p>
            </div>
          </div>
        </div>
    </body>
</html>
