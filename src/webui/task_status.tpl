{{>HEADER}}

{{>PAGE_HEADER}}

<script type="text/javascript">
var runtime_series = [];
var llc_miss_series = [];
var rsize_ts = [];

function getRAM(data) {
  rsize_ts = [];
  for (i = 0; i < data.length; i++) {
    rsize_ts.push(data[i].rsize / 1024.0 / 1024.0)
  }
}

function getReportStats(data) {
  runtime_series = [];
  llc_miss_series = [];
  for (i = 0; i < data.length; i++) {
    runtime_series.push(data[i].runtime);
    llc_miss_series.push(data[i].llc_misses);
  }
}

function updateGraphs(data) {
  getRAM(data['samples']);
  getReportStats(data['reports']);
}

function poll() {
  url = "/stats/?task={{TASK_ID}}";
  $.ajax({
    url: url,
    async: false,
    dataType: 'json',
    success: function(data) {
      updateGraphs(data);
    }});
}

function step() {
  poll();
  $('#runtime-box').sparkline(runtime_series, {type: 'box', width: '50px'});
  $('#llc-miss-box').sparkline(llc_miss_series, {type: 'box', width: '50px'});
  $('#rsize-ts').sparkline(rsize_ts, {tooltipSuffix: ' MB'});
  $('#rsize-box').sparkline(rsize_ts, {type: 'box', width: '50px'});
  window.setTimeout(step, 10000);
}

$(function() {
  step();
});
</script>


<h1>Task {{TASK_ID}}</h1>

<table border="1">
  <tr>
    <td>ID</td>
    <td>{{TASK_ID}}</td>
  </tr>
  <tr>
    <td>Name</td>
    <td>{{TASK_NAME}}</td>
  </tr>
  <tr>
    <td>Code/binary</td>
    <td>{{TASK_BINARY}}</td>
  </tr>
  <tr>
    <td>Equiv class</td>
    <td>{{TASK_TEC}}</td>
  </tr>
  <tr>
    <td>Status</td>
    <td>{{TASK_STATUS}}</td>
  </tr>
  <tr>
    <td>Dependencies</td>
    <td>
      <ol>
        {{#TASK_DEPS}}
        <li><a href="/ref/?id={{TASK_DEP_ID}}">{{TASK_DEP_ID}}</a></li>
        {{/TASK_DEPS}}
      </ol>
    </td>
  </tr>
  <tr>
    <td>Spawned</td>
    <td>
      <ol>
        {{#TASK_SPAWNED}}
        <li><a href="/task/?id={{TASK_SPAWNED_ID}}">{{TASK_SPAWNED_ID}}</a></li>
        {{/TASK_SPAWNED}}
      </ol>
    </td>
  </tr>
  <tr>
    <td>Outputs</td>
    <td>
      <ol>
      {{#TASK_OUTPUTS}}
        <li><a href="/ref/?id={{TASK_OUTPUT_ID}}">{{TASK_OUTPUT_ID}}</a></li>
      {{/TASK_OUTPUTS}}
      </ol>
    </td>
  </tr>
  <tr>
    <td>Actions</td>
    <td>
      <a href="/task/?id={{TASK_ID}}&a=kill">Kill</a></li>
    </td>
  </tr>
  <tr>
    <td>Runtime</td>
    <td><span id="runtime-box">Waiting for data...</span></td>
  </tr>
  <tr>
    <td>LLC misses</td>
    <td><span id="llc-miss-box">Waiting for data...</span></td>
  </tr>
  <tr>
    <td>Resident memory</td>
    <td><span id="rsize-ts">Waiting for data...</span></td>
  </tr>
  <tr>
    <td></td>
    <td><span id="rsize-box">Waiting for data...</span></td>
  </tr>
</table>

{{>PAGE_FOOTER}}
