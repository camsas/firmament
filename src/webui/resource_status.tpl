{{>HEADER}}

{{>PAGE_HEADER}}

<h1>{{RES_ID}}</h1>

<script type="text/javascript">
var ramTimeseries;
var ramPercentTimeseries;
var cpuAggUsrTimeseries;
var cpuAggSysTimeseries;

function getRAM(data) {
  var ts1 = [];
  var ts2 = [];
  for (i = 0; i < data.length; i++) {
    ts2.push((data[i].total_ram - data[i].free_ram) /
             data[i].total_ram);
    ts1.push((data[i].total_ram - data[i].free_ram) / 1024.0 / 1024.0)
  }
  ramTimeseries = ts1;
  ramPercentTimeseries = ts2;
}

function getCPU(data) {
  var ts1 = [];
  var ts2 = [];
  for (i = 0; i < data.length; i++) {
    ts1.push(data[i].cpus_usage[0].user);
    ts2.push(data[i].cpus_usage[0].system)
  }
  cpuAggUsrTimeseries = ts1;
  cpuAggSysTimeseries = ts2;
}


function updateGraphs(data) {
  getRAM(data);
  getCPU(data);
}

function poll() {
  url = "/stats/?res={{RES_ID}}";
  $.ajax({
    url: url,
    async: false,
    cache: false,
    dataType: 'json',
    success: function(data) {
      console.debug("Poll succeeded -- updating graphs.");
      updateGraphs(data);
    },
    error: function(jqxhr, text_status, error_thrown) {
      console.error("Failed to poll statistics: " + text_status);
    }
  });
}

function step() {
  console.debug("Polling statistics...");
  poll();
  $('#ram-sparkline').sparkline(ramTimeseries, {tooltipSuffix: ' MB'});
  $('#ram-perc-sparkline').sparkline(ramPercentTimeseries, {chartRangeMin: 0.0, chartRangeMax: 1.0});
  $('#ram-sparkline').sparkline(ramTimeseries, {tooltipSuffix: ' MB'});
  $('#cpu-agg-sys').sparkline(cpuAggSysTimeseries, {lineColor: '#ff0000', fillColor: '#ffaaaa'});
  $('#cpu-agg-usr').sparkline(cpuAggUsrTimeseries, {lineColor: '#00ff00', fillColor: '#aaffaa'});
  // update timers
  $("abbr.timeago").each(function (index) {
    $(this).text(jQuery.timeago(new Date(this.title)));
  });
  window.setTimeout(step, 10000);
}

$(function() {
  $.fn.sparkline.defaults.line.height = '50px';
  $("abbr.timeago").each(function (index) {
    $(this).attr("title", new Date(parseInt(this.title)).toISOString());
  });
  step();
});
</script>

<table class="table table-bordered">
  <tr>
    <td>ID</td>
    <td>{{RES_ID}}</td>
  </tr>
  <tr>
    <td>Friendly name</td>
    <td>{{RES_FRIENDLY_NAME}}</td>
  </tr>
  <tr>
    <td>Equivalence class</td>
    <td>
      <ul>
        {{#RES_RECS}}
        <li><a href="/ec/?id={{RES_REC}}">{{RES_REC}}</a></li>
        {{/RES_RECS}}
      </ul>
    </td>
  </tr>
  <tr>
    <td>Type</td>
    <td>{{RES_TYPE}}</td>
  </tr>
  <tr>
    <td>Status</td>
    <td>{{RES_STATUS}}</td>
  </tr>
  <tr>
    <td>Parent ID</td>
    <td><a href="/resource/?id={{RES_PARENT_ID}}">{{RES_PARENT_ID}}</a></td>
  </tr>
  <tr>
    <td>Children IDs</td>
    <td>
      Total {{RES_NUM_CHILDREN}}:
      <ul>
      {{#RES_CHILDREN}}
        <li><a href="/resource/?id={{RES_CHILD_ID}}">{{RES_CHILD_ID}}</a></li>
      {{/RES_CHILDREN}}
      </ul>
    </td>
  </tr>
  <tr>
    <td>Last location</td>
    <td><a href="http://{{RES_LOCATION_HOST}}:{{WEBUI_PORT}}/resource/?id={{RES_ID}}">{{RES_LOCATION}}</a></td>
  </tr>
  <tr>
    <td>Last heartbeat</td>
    <td><abbr class="timeago" title="{{RES_LAST_HEARTBEAT}}">{{RES_LAST_HEARTBEAT}}</a></td>
  </tr>
  <tr>
    <td>CPU (usr)</td>
    <td><span id="cpu-agg-usr">Waiting for data...</span></td>
  </tr>
  <tr>
    <td>CPU (sys)</td>
    <td><span id="cpu-agg-sys">Waiting for data...</span></td>
  </tr>
  <tr>
    <td>RAM in use</td>
    <td><span id="ram-sparkline">Waiting for data...</span></td>
  </tr>
  <tr>
    <td>RAM % in use</td>
    <td><span id="ram-perc-sparkline">Waiting for data...</span></td>
  </tr>
</table>

{{>PAGE_FOOTER}}
