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
        <style>
        .transmitting_missed_allowed_count {
            background-color: #ff3633;
          }
          .transmitting_missed {
            background-color: #fff45c;
          }

        </style>
        <div class="container">
            <div class="row">
                <h1>NERRS Telemetry Daily Uptime Report</h1>
                <h2>Report Period: ${times[0]} to ${times[1]}</h2>
                <h3>This report shows the percentage of received telemetry transmissions</h3>
                </br>
            </div>
            <div class="row">
                <table class="table table-bordered">
                    <tr>
                        <th>Station Code</th>
                        <th>Receive Percentage</th>
                    </tr>
                    % for station_code in station_codes:
                        % if station_stats[station_code] < 50:
                        <tr class="transmitting_missed_allowed_count">
                        % else:
                        <tr>
                        % endif
                            <td>
                                ${ station_code }
                            </td>
                            <td>
                              ${ station_stats[station_code] }
                            </td>
                        </tr>
                    % endfor
                </table>
            </div>
        </div>
    </body>
</html>
