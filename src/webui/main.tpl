{{>HEADER}}

{{>PAGE_HEADER}}

<h1>Firmament coordinator</h1>

<p><strong>ID:</strong> {{COORD_ID}}</p>
<p><strong>Hostname:</strong> {{COORD_HOST}}</p>
{{#COORD_HAS_PARENT}}
<p><strong>Parent:</strong> {{COORD_PARENT_URI}} (<a href="http://{{COORD_PARENT_HOST}}:{{WEBUI_PORT}}">{{COORD_PARENT_HOST}}</a>)</p>
{{/COORD_HAS_PARENT}}
<p><strong>Logs:</strong> <a href="/log?log=ERROR">ERROR</a> &ndash; <a href="/log?log=WARNING">WARNING</a> &ndash; <a href="/log?log=INFO">INFO</a></p>

<h2>Overview</h2>
<table class="table table-bordered">
  <thead>
    <tr>
      <th>Jobs [running]</th>
      <th>Tasks [running]</th>
      <th>Resources [local]</th>
      <th>References [concrete]</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="/jobs">{{NUM_JOBS_KNOWN}}</a> [{{NUM_JOBS_RUNNING}}]</td>
      <td><a href="/tasks">{{NUM_TASKS_KNOWN}}</a> [{{NUM_TASKS_RUNNING}}]</td>
      <td><a href="/resources">{{NUM_RESOURCES_KNOWN}}</a> [{{NUM_RESOURCES_LOCAL}}]</td>
      <td><a href="/refs">{{NUM_REFERENCES_KNOWN}}</a> [<a href="/refs?filter=0">{{NUM_REFERENCES_CONCRETE}}</a>]</td>
    </tr>
  </tbody>
</table>

<h2>Scheduler</h2>

<p><b>Active scheduler:</b> {{SCHEDULER_NAME}}

<ol>
{{#SCHEDULER_ITER}}
  <li>Iteration {{SCHEDULER_ITER_ID}} &ndash; flow graph (<a href="/sched/?iter={{SCHEDULER_ITER_ID}}&a=dimacs">DIMACS</a>;
                                                          <a href="/sched/?iter={{SCHEDULER_ITER_ID}}&a=gv">GV</a>;
                                                          <a href="/sched/?iter={{SCHEDULER_ITER_ID}}&a=png">PNG</a>)</li>
{{/SCHEDULER_ITER}}
</ol>

{{>PAGE_FOOTER}}
