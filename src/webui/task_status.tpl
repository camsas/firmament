{{>HEADER}}

{{>PAGE_HEADER}}

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
    <td>Status</td>
    <td>{{TASK_STATUS}}</td>
  </tr>
  <tr>
    <td rowspan="{{TASK_NUM_DEPS}}">Dependencies</td>
    {{#TASK_DEPS}}
    <td><a href="/ref/?id={{TASK_DEP_ID}}">{{TASK_DEP_ID}}</a></td>
    {{/TASK_DEPS}}
  </tr>
  <tr>
    <td rowspan="{{TASK_NUM_SPAWNED}}">Spawned</td>
    {{#TASK_SPAWNED}}
    <td><a href="/task/?id={{TASK_SPAWNED_ID}}">{{TASK_SPAWNED_ID}}</a></td>
    {{/TASK_SPAWNED}}
  </tr>
  <tr>
    <td rowspan="{{TASK_NUM_OUTPUTS}}">Outputs</td>
    {{#TASK_OUTPUTS}}
    <td><a href="/ref/?id={{TASK_OUTPUT_ID}}">{{TASK_OUTPUT_ID}}</a></td>
    {{/TASK_OUTPUTS}}
  </tr>
</table>

{{>PAGE_FOOTER}}
