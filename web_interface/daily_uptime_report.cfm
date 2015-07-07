<cfscript>
    variables.config_file="D:\\scripts\\telemetry_alerts\\telemetry_alerts.ini";
    variables.arguments = ArrayNew(1);
    ArrayAppend(variables.arguments, "D:\\scripts\\telemetry_alerts\\create_uptime_report.py");
    ArrayAppend(variables.arguments, "--ConfigFile=#variables.config_file#");
    ArrayAppend(variables.arguments, "--ReportTemplate=D:\\scripts\\telemetry_alerts\\report_template\\daily_telemetry_uptime.mako");
    ArrayAppend(variables.arguments, "--PageFile=C:\\inetpub\\wwwroot\\telemetry_alerts\\data\\daily_uptime.html");
</cfscript>

<cfexecute name="D:\\python27\\python.exe"
            arguments=#variables.arguments#
            timeout="2"
            variable="scripts_results">
</cfexecute>

