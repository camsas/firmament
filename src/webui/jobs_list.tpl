{{>HEADER}}

{{>PAGE_HEADER}}

<h1>Jobs</h1>

<table class="table table-bordered">
  <thead>
    <tr>
      <th>#</th>
      <th>Job ID</th>
      <th>Friendly name</th>
      <th>State</th>
      <th>Root task</th>
      <th>Options</th>
    </tr>
  </thead>
  <tbody>
  {{#JOB_DATA}}
    <tr>
      <td>{{JOB_NUM}}</td>
      <td>{{JOB_ID}}</td>
      <td>{{JOB_FRIENDLY_NAME}}</td>
      <td>{{JOB_STATE}}</td>
      <td><a href="/task/?id={{JOB_ROOT_TASK_ID}}">{{JOB_ROOT_TASK_ID}}</a></td>
      <td>
        <a href="/job/status/?id={{JOB_ID}}"><span class="glyphicon glyphicon-th-list" aria-hidden="true" title="Status"></span></a> 
        <a href="/job/completion/?id={{JOB_ID}}"><span class="glyphicon glyphicon-bell" aria-hidden="true" title="Alert"></span></a> 
        <a href="/job/kill/?id={{JOB_ID}}"><span class="glyphicon glyphicon-trash" aria-hidden="true" title="Terminate"></span></a> 
        <a href="/job/dtg-view/?id={{JOB_ID}}"><span class="glyphicon glyphicon-search" aria-hidden="true" title="DTG"></span></a>
      </td>
    </tr>
  {{/JOB_DATA}}
  </tbody>
</table>

{{>PAGE_FOOTER}}
