{{>HEADER}}

{{>PAGE_HEADER}}

<h1>Coordinator {{COORD_ID}}</h1>

<table border="1" style="text-align: center;">
  <tr>
    <th>Jobs [running]</th>
    <th>Tasks [running]</th>
    <th>Resources [local]</th>
    <th>References [concrete]</th>
  </tr>
  <tr>
    <td><a href="/jobs">{{NUM_JOBS_KNOWN}}</a> [{{NUM_JOBS_RUNNING}}]</td>
    <td>{{NUM_TASKS_KNOWN}} [{{NUM_TASKS_RUNNING}}]</td>
    <td>{{NUM_RESOURCES_KNOWN}} [<a href="/resources">{{NUM_RESOURCES_LOCAL}}</a>]</td>
    <td><a href="/refs">{{NUM_REFERENCES_KNOWN}}</a> [<a href="/refs?filter=0">{{NUM_REFERENCES_CONCRETE}}</a>]</td>
  </tr>
</table>

{{>PAGE_FOOTER}}
