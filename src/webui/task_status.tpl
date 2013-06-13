{{>HEADER}}

{{>PAGE_HEADER}}

<script type="text/javascript">
var rsizeTimeseries;

function getRAM(data) {
  var ts1 = [];
  for (i = 0; i < data.length; i++) {
    ts1.push(data[i].rsize / 1024.0 / 1024.0)
  }
  ramTimeseries = ts1;
}

function updateGraphs(data) {
  getRAM(data);
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
  $('#rsize-ts').sparkline(ramTimeseries, {tooltipSuffix: ' MB'});
  $('#rsize-box').sparkline(ramTimeseries, {type: 'box', width: '50px'});
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
    <td>Resident memory</td>
    <td><span id="rsize-ts">Waiting for data...</span></td>
  </tr>
  <tr>
    <td></td>
    <td><span id="rsize-box">Waiting for data...</span></td>
  </tr>
</table>

{{>PAGE_FOOTER}}
