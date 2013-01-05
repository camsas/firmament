{{>HEADER}}

{{>PAGE_HEADER}}

<h1>Coordinator {{COORD_ID}}</h1>

<table border="1" style="text-align: center;">
  <tr>
    <th>Jobs [running]</th>
    <th>Tasks [running]</th>
    <th>Resources [local]</th>
  </tr>
  <tr>
    <td><a href="/jobs">{{NUM_JOBS_KNOWN}}</a> [{{NUM_JOBS_RUNNING}}]</td>
    <td>{{NUM_TASKS_KNOWN}} [{{NUM_TASKS_RUNNING}}]</td>
    <td>{{NUM_RESOURCES_KNOWN}} [<a href="/resources">{{NUM_RESOURCES_LOCAL}}</a>]</td>
  </tr>
</table>

{{>PAGE_FOOTER}}
