{{>HEADER}}

{{>PAGE_HEADER}}

<h1>References known to object store</h1>

<table border="1">
  <tr>
<!--    <th>Object ID</th>-->
    <th>Type</th>
    <th>Producing task</th>
    <th>Location</th>
  </tr>
  {{#OBJ_DATA}}
  <tr>
    <th colspan="3"><a href="/ref/?id={{OBJ_ID}}">{{OBJ_ID}}</a></th>
  </tr>
  {{#REF_DATA}}
  <tr>
    <td>{{REF_TYPE}}</td>
    <td><a href="/task/?id={{REF_PRODUCING_TASK_ID}}">{{REF_PRODUCING_TASK_ID}}</a></td>
    <td>{{REF_LOCATION}}</td>
  </tr>
  {{/REF_DATA}}
  {{/OBJ_DATA}}
</table>

{{>PAGE_FOOTER}}
