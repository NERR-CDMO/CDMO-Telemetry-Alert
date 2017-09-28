function station_settings(init_data)
{
  var self = this;

  self.station_code = init_data.metadata.station_code || "";
  self.hads_id = init_data.metadata.hads_id || "";
  self.test_enabled = ko.observable(init_data.status.disable_test ? false : true || "");
  self.allowed_hour_count_to_miss = ko.observable(init_data.status.allowed_hour_count_to_miss || "");
  self.current_hour_count_missed = init_data.status.current_hour_count_missed || "";
  self.last_update_time = ko.observable(init_data.status.last_update_time || "");
  self.last_check_status_time = ko.observable(init_data.status.last_check_status_time || "");
  self.status_field_text = ko.observable(init_data.status.status_field_text || "");
  //self.transmit_time = ko.observable(init_data.metadata.transmit_time || "");
  //self.export_time = ko.observable(init_data.metadata.export_time || "");
  self.updated = false;

  self.test_enabled.subscribe(function(value) {
    self.updated = true;
  });
  self.allowed_hour_count_to_miss.subscribe(function(value) {
    self.updated = true;
  });

  self.get_status_class = function()
  {
    var status_class = "";
    if(self.allowed_hour_count_to_miss() < self.current_hour_count_missed)
    {
      status_class = "transmitting_missed_allowed_count";
    }
    else if(self.current_hour_count_missed)
    {
      status_class = "transmitting_missed";
    }
    return status_class;
  };

  return(self);
};

function popup() {
  var self = this;

  self.popup_title = ko.observable("");
  self.popup_message = ko.observable("");

  return(self);
};

function telemetry_alert_view_model() {
  var self = this;

  self.station_data = ko.observableArray();
  self.popup_view = new popup();

  self.initialize = function(options)
  {
    if('test_data' in options)
    {
      self.telemetry_alerts_data(options.test_data)
    }
  };
  self.telemetry_alerts_data = function(json_data)
  {
    $.each(json_data, function(station_code, data_rec)
    {
      //self.station_data().push(data_rec);
      self.station_data.push(new station_settings(data_rec));
    });
  };
  self.save_changes = function()
  {
    var json_data = {};
    $.each(self.station_data(), function(ndx, station_data_rec)
    {
      if(station_data_rec.updated) {
        json_data[station_data_rec.station_code] = {
          "test_disabled": !station_data_rec.test_enabled(),
          "allowed_hour_count_to_miss": parseInt(station_data_rec.allowed_hour_count_to_miss())
        };
      }
    });
    $.ajax({
        url: 'telemetry_alerts_request_handler.cfm',
        type: "POST",
        data: JSON.stringify(json_data),
        contentType: "application/json",
        complete: function(data,status)
        {
          self.popup_view.popup_title("Save");
          self.popup_view.popup_message(data.responseText);
          $('#message_popup').modal("show");
        }
    });
  };

  return(self);
};

