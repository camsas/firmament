{{>HEADER}}

{{>PAGE_HEADER}}

<h1>References known to object store</h1>

<table border="1">
  <tr>
    <th>Object ID</th>
    <th>Type</th>
    <th>Producing task</th>
  </tr>
  {{#REF_DATA}}
  <tr>
    <td>{{REF_ID}}</td>
    <td>{{REF_TYPE}}</td>
    <td><a href="/task/?id={{REF_PRODUCING_TASK_ID}}">{{REF_PRODUCING_TASK_ID}}</a></td>
  </tr>
  {{/REF_DATA}}
</table>

{{>PAGE_FOOTER}}
