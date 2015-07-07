<!--- Have to use URLEncodeFOrmat otherwise the cfexecute strips off the quotes when passing the json data. --->

<cfset requestBody = URLEncodedFormat(toString( getHttpRequestData().content )) />

<cfscript>
    variables.config_file="D:\\scripts\\telemetry_alerts\\telemetry_alerts.ini";
    variables.arguments = ArrayNew(1);
    ArrayAppend(variables.arguments, "D:\\scripts\\telemetry_alerts\\telemetry_alerts_request_handler.py");
    ArrayAppend(variables.arguments, "--ConfigFile=#variables.config_file#");
    ArrayAppend(variables.arguments, "--JsonData=#requestBody#");
</cfscript>

<cfexecute name="D:\\python27\\python.exe"
            arguments=#variables.arguments#
            timeout="2"
            variable="scripts_results">
</cfexecute>


<cfoutput>
    #scripts_results#
</cfoutput>