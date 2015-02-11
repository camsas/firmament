{{>HEADER}}

{{>PAGE_HEADER}}

<h1>Job {{JOB_ID}}</h1>

<table class="table table-bordered">
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
    <td>Outputs</td>
    <td>
      <ul>
      {{#JOB_OUTPUTS}}
      <li><a href="/ref/?id={{JOB_OUTPUT_ID}}">{{JOB_OUTPUT_ID}}</a></li>
      {{/JOB_OUTPUTS}}
      </ul>
    </td>
  </tr>
</table>

{{>PAGE_FOOTER}}
