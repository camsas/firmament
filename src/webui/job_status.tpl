{{>HEADER}}

{{>PAGE_HEADER}}

<h1>Job {{JOB_ID}}</h1>

<table border="1">
  <tr>
    <td>ID</td>
    <td>{{JOB_ID}}</td>
  </tr>
  <tr>
    <td>Name</td>
    <td>{{JOB_NAME}}</td>
  </tr>
  <tr>
    <td>Status</td>
    <td>{{JOB_STATUS}}</td>
  </tr>
  <tr>
    <td>Root task</td>
    <td><a href="/task/?id={{JOB_ROOT_TASK_ID}}">{{JOB_ROOT_TASK_ID}}</a></td>
  </tr>
  <tr>
    <td rowspan="{{JOB_NUM_OUTPUTS}}">Outputs</td>
    {{#JOB_OUTPUTS}}
    <td><a href="/ref/?id={{JOB_OUTPUT_ID}}">{{JOB_OUTPUT_ID}}</a></td>
    {{/JOB_OUTPUTS}}
  </tr>
</table>

{{>PAGE_FOOTER}}
