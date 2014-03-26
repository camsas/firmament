{{>HEADER}}

{{>PAGE_HEADER}}

<script type="text/javascript">
var runtime_series = [];
var cycles_series = [];
var instructions_series = [];
var cpi_series = [];
var mai_series = [];
var ipma_series = [];
var llc_refs_series = [];
var llc_miss_series = [];
var rsize_ts = [];
var sched_run_ts = [];
var sched_wait_runnable_ts = [];

function mean(values) {
  var sum = 0;
  for (var i = 0; i < values.length; i++) {
    sum += parseInt(values[i]);
  }
  return sum / values.length;
}

function median(values) {
  values.sort( function(a,b) {return a - b;} );
  var half = Math.floor(values.length/2);
  if(values.length % 2)
    return values[half];
  else
    return (values[half-1] + values[half]) / 2.0;
}

function getRAM(data) {
  rsize_ts = [];
  for (i = 0; i < data.length; i++) {
    rsize_ts.push(data[i].rsize / 1024.0 / 1024.0)
  }
}

function getSchedStats(data) {
  sched_run_ts = [];
  sched_wait_runnable_ts = [];
  for (i = 0; i < data.length; i++) {
    sched_run_ts.push(data[i].sched_run);
    sched_wait_runnable_ts.push(data[i].sched_wait);
  }
}

function getReportStats(data) {
  runtime_series = [];
  cycles_series = [];
  instructions_series = [];
  cpi_series = [];
  mai_series = [];
  ipma_series = [];
  llc_ref_series = [];
  llc_miss_series = [];
  for (i = 0; i < data.length; i++) {
    runtime_series.push(data[i].runtime);
    cycles_series.push(data[i].cycles);
    instructions_series.push(data[i].instructions);
    cpi_series.push(data[i].cycles / data[i].instructions);
    ipma_series.push(data[i].instructions / data[i].llc_refs);
    mai_series.push(data[i].llc_refs / data[i].instructions);
    llc_ref_series.push(data[i].llc_refs);
    llc_miss_series.push(data[i].llc_misses);
  }
}

function updateGraphs(data) {
  getRAM(data['samples']);
  getSchedStats(data['samples']);
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
  // whisker bars
  $('#runtime-box').sparkline(runtime_series, {type: 'box', width: '100px'});
  $('#cycles-box').sparkline(cycles_series, {type: 'box', width: '100px'});
  $('#instructions-box').sparkline(instructions_series, {type: 'box', width: '100px'});
  $('#cpi-box').sparkline(cpi_series, {type: 'box', width: '100px'});
  $('#ipma-box').sparkline(ipma_series, {type: 'box', width: '100px'});
  $('#mai-box').sparkline(mai_series, {type: 'box', width: '100px'});
  $('#llc-ref-box').sparkline(llc_ref_series, {type: 'box', width: '100px'});
  $('#llc-miss-box').sparkline(llc_miss_series, {type: 'box', width: '100px'});
  $('#rsize-ts').sparkline(rsize_ts, {tooltipSuffix: ' MB'});
  $('#rsize-box').sparkline(rsize_ts, {type: 'box', width: '100px'});
  $('#sched_run-ts').sparkline(sched_run_ts, {tooltipSuffix: ' '});
  $('#sched_wait_runnable-ts').sparkline(sched_wait_runnable_ts, {tooltipSuffix: ' '});
  // labels
  $('#runtime-text').text(" avg: " + mean(runtime_series) + ", median: " + median(runtime_series));
  $('#cycles-text').text(" avg: " + mean(cycles_series) + ", median: " + median(cycles_series));
  $('#instructions-text').text(" avg: " + mean(instructions_series) + ", median: " + median(instructions_series));
  $('#cpi-text').text(" avg: " + mean(cpi_series) + ", median: " + median(cpi_series));
  $('#ipma-text').text(" avg: " + mean(ipma_series) + ", median: " + median(ipma_series));
  $('#mai-text').text(" avg: " + mean(mai_series) + ", median: " + median(mai_series));
  $('#llc-ref-text').text(" avg: " + mean(llc_ref_series) + ", median: " + median(llc_ref_series));
  $('#llc-miss-text').text(" avg: " + mean(llc_miss_series) + ", median: " + median(llc_miss_series));
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
    <td><span id="runtime-box">Waiting for data...</span> <span id="runtime-text" /></td>
  </tr>
  <tr>
    <td>Cycles</td>
    <td><span id="cycles-box">Waiting for data...</span> <span id="cycles-text" /></td>
  </tr>
  <tr>
    <td>Instructions</td>
    <td><span id="instructions-box">Waiting for data...</span> <span id="instructions-text" /></td>
  </tr>
  <tr>
    <td>CPI</td>
    <td><span id="cpi-box">Waiting for data...</span> <span id="cpi-text" /></td>
  </tr>
  <tr>
    <td>IPMA</td>
    <td><span id="ipma-box">Waiting for data...</span> <span id="ipma-text" /></td>
  </tr>
  <tr>
    <td>MAI</td>
    <td><span id="mai-box">Waiting for data...</span> <span id="mai-text" /></td>
  </tr>
  <tr>
    <td>LLC references</td>
    <td><span id="llc-ref-box">Waiting for data...</span> <span id="llc-ref-text" /></td>
  </tr>
  <tr>
    <td>LLC misses</td>
    <td><span id="llc-miss-box">Waiting for data...</span> <span id="llc-miss-text" /></td>
  </tr>
  <tr>
    <td>Resident memory</td>
    <td><span id="rsize-ts">Waiting for data...</span></td>
  </tr>
  <tr>
    <td></td>
    <td><span id="rsize-box">Waiting for data...</span></td>
  </tr>
  <tr>
    <td>Time running on CPU per heartbeat interval (1s)</td>
    <td><span id="sched_run-ts">Waiting for data...</span></td>
  </tr>
  <tr>
    <td>Time waiting for CPU(while runnable) per heartbeat interval (1s)</td>
    <td><span id="sched_wait_runnable-ts">Waiting for data...</span></td>
  </tr>
</table>

{{>PAGE_FOOTER}}
