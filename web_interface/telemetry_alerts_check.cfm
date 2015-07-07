<cfscript>
    variables.config_file="D:\\scripts\\telemetry_alerts\\telemetry_alerts.ini";
    variables.arguments = ArrayNew(1);
    ArrayAppend(variables.arguments, "D:\\scripts\\telemetry_alerts\\telemetry_alerts.py");
    ArrayAppend(variables.arguments, "--ConfigFile=#variables.config_file#");
    ArrayAppend(variables.arguments, "--CheckStatus");
</cfscript>

<cfexecute name="D:\\python27\\python.exe"
            arguments=#variables.arguments#
            timeout="2"
            variable="scripts_results">
</cfexecute>

