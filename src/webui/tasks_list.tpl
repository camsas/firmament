{{>HEADER}}

{{>PAGE_HEADER}}

<script type="text/javascript">
$(function(){
  $("#task-table").tablesorter({
    theme : "bootstrap",
    widthFixed: true,
    headerTemplate : '{content} {icon}',
    widgets : [ "uitheme", "filter", "zebra" ],
    widgetOptions : {
      zebra : ["even", "odd"],
      filter_columnFilters : true,
      filter_filteredRow : 'filtered',
      filter_hideEmpty : true,
      filter_hideFilters : true,
      filter_ignoreCase : true,
      filter_liveSearch : true,
      filter_searchDelay : 300,
      filter_cssFilter: "form-control",
    }
  });
});
</script>

<h1>Tasks</h1>

<table class="table table-bordered tablesorter" id="task-table">
  <thead>
    <tr>
      <th>Task ID</th>
      <th>Friendly name</th>
      <th>Job ID</th>
      <th>State</th>
      <th>Resource</th>
      <th>Options</th>
    </tr>
  </thead>
  <tbody>
  {{#TASK_DATA}}
    <tr>
      <td><a href="/task/?id={{TASK_ID}}">{{TASK_ID}}</a></td>
      <td>{{TASK_FRIENDLY_NAME}}</td>
      <td><a href="/job/status/?id={{TASK_JOB_ID}}">{{TASK_JOB_ID}}</a></td>
      <td>{{TASK_STATE}}</td>
      <td><a href="/resource/status/?id={{TASK_RESOURCE}}">{{TASK_RESOURCE}}</a></td>
      <td>
        <a href="/task/?id={{TASK_ID}}"><span class="glyphicon glyphicon-th-list" aria-hidden="true" title="Status"></span></a> 
        <a href="/task/?id={{TASK_ID}}&a=kill"><span class="glyphicon glyphicon-trash" aria-hidden="true" title="Terminate"></span></a>
      </td>
    </tr>
  {{/TASK_DATA}}
  </tbody>
</table>

{{>PAGE_FOOTER}}
